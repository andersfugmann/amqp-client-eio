(** Module to register and handle amqp messages.
    The Service module allows registering requests and handlers for incoming messages.

    This module is not thread safe
*)

(* We need to make this module threadsafe wrt. simple calls - not registrations *)


open! StdLabels
module Queue = Stdlib.Queue
open Utils

(* Need a list of waiters. We should be able to front and back insert on this,
   but it might not be necessary to front insert. Lets see! *)
module Message_table = Hashtbl.Make(Types.Message_id)

type expect = (Types.Message_id.t * Cstruct.t, exn) result -> unit
type 'a content = { content: 'a; body : Cstruct.t list }

type t = {
  send_stream: Cstruct.t Stream.t;
  receive_stream: (Types.Frame_type.t * Cstruct.t) Stream.t;
  expect: expect Queue.t;
  services: (Cstruct.t -> unit) Message_table.t;
  channel_no: int;
  client_lock: Mutex.t;
}

let handle_method t data =
  let message_id, data = Framing.decode_method_header data in
  match Message_table.find_opt t.services message_id with
  | Some handler ->
    handler data
  | None ->
    with_lock t.client_lock @@ fun () ->
    let handler = Queue.pop t.expect in
    handler (Ok (message_id, data))

(** Construct a new service. *)
let init ~send_stream ~receive_stream ~channel_no () =
  {
    send_stream;
    receive_stream;
    expect = Queue.create ();
    services = Message_table.create 7;
    channel_no;
    client_lock = Mutex.create ();
  }

type 'r response =
  | Method of (Cstruct.t -> unit -> 'r)
  | Method_content of int * (Cstruct.t -> Framing.content -> unit -> 'r)

let expect_method:
  ('rep_m, _, _, _, _, _, _, _, _, _) Protocol.Spec.def ->
  ('rep_m -> 'rep) -> Types.Message_id.t * 'rep response = fun def rep_f ->
  let read = Protocol.Spec.read def.spec in
  def.message_id, Method (fun data () -> rep_f (read def.make data 0))

let expect_method_content:
  ('rep_m, _, _, _, _, _, _, _, _, _) Protocol.Spec.def ->
  ('rep_c, _, _, _, _, _, _, _, _, _) Protocol.Content.def ->
  ('rep_m * ('rep_c * Cstruct.t list) -> 'rep) -> Types.Message_id.t * 'rep response = fun def_m def_c rep_f ->
  let decode_method = Framing.decode_method def_m in
  let decode_content = Framing.decode_content def_c in
  let read method_data content_data () =
    let method_  = decode_method method_data in
    let content = decode_content content_data in
    rep_f (method_, content)
  in
  def_m.message_id, Method_content (def_c.message_id.class_id, read)


(* optimization:
   Apply loop unrolling from zero to 2 elements before reading the key to be looked up, we don't need to traverse
   the list in memory for each lookup
*)
let rec list_assoc_map: ('key * 'data) list -> key:'key -> f:('data -> 'b) -> default:(unit -> 'b) -> 'b = fun l ->
  match l with
  | [] -> fun ~key:_ ~f:_ ~default -> default ()
  | [ k, v ] -> fun ~key ~f ~default ->
    if k = key then
      f v
    else
      default ()
  | [ k1, v1; k2, v2 ] -> fun ~key ~f ~default ->
    if k1 = key then
      f v1
    else if k2 = key then
      f v2
    else
      default ()
  | (k, v) :: xs ->
    let tail = list_assoc_map xs in
    fun ~key ~f ~default ->
      if k = key then
        f v
      else
        tail ~key ~f ~default

(* I might agree that static initialization is overdone and performance gain is questionable.
   But hey, its fun and I actually think *some* of the code is actually more readable
*)

(** Initiate request response towards the server.
    Messages are decoded in the caller context.
    This function is thread safe.
*)
let client_request_response:
  ('req, _, _, _, 'make_named, 'res, _, _, _, _) Protocol.Spec.def ->
  (Types.message_id * 'res response) list -> t -> 'make_named = fun req ress ->
  let create_request = Framing.create_method_frame req in
  let handlers = list_assoc_map ress in
  let handler t data = function
    | Method f -> f data
    | Method_content (class_id, f) ->
      let content = Framing.read_content t.receive_stream in
      assert (content.header.class_id = class_id);
      f data content
  in

  let expect t promise = function
    | Ok (message_id, data) -> begin
        try
          handlers ~key:message_id ~f:(handler t data) ~default:(fun () ->
            failwith_f "Message id %s not in [%s]"
              (Types.Message_id.to_string message_id)
              (List.map ~f:(fun (id, _) -> Types.Message_id.to_string id) ress |> String.concat ~sep:"; ")
          ) |> Promise.resolve_ok promise
        with
        | Failure _ as exn -> raise exn
        | exn -> Promise.resolve_exn promise exn; raise exn
      end
    | Error exn ->
      Promise.resolve_exn promise exn
  in

  let send_request = match ress with
    | [] -> fun t _p request -> Stream.send t.send_stream request
    | _ ->  fun t p request ->
      with_lock t.client_lock @@ fun () ->
      Queue.push (expect t p) t.expect;
      Stream.send t.send_stream request
  in

  let call t req =
    let u, p = Promise.create () in
    let request = create_request ~channel_no:t.channel_no req in
    send_request t p request;
    let f = Promise.await u in
    f ()
  in
  fun t ->
    req.make_named (call t)


let server_request:
  ('req, _, _, _, _, _, _, _, _, _) Protocol.Spec.def ->
  t -> ('req -> unit) -> unit = fun req ->
  let decode_request = Framing.decode_method req in
  let handler f data =
    decode_request data
    |> f
  in
  fun t f ->
    Message_table.add t.services req.message_id (handler f)


(* Content will not be decoded *)
let server_request_content:
  ('req, _, _, _, _, _, _, _, _, _) Protocol.Spec.def ->
  ('content, _, _, _, _, _, _, _, _, _) Protocol.Content.def ->
  t -> ('req -> 'content -> Cstruct.t list -> unit) -> unit = fun req content ->
  let decode_request = Framing.decode_method req in
  let decode_content = Framing.decode_content content in
  let handler t f data =
    let request = decode_request data in
    let content, body =
      Framing.read_content t.receive_stream
      |> decode_content
    in
    f request content body
  in
  fun t f ->
    Message_table.add t.services req.message_id (handler t f)

let server_request_response:
  ('req, _, _, _, _, _, _, _, _, _) Protocol.Spec.def ->
  ('res, _, _, _, _, _, _, _, _, _) Protocol.Spec.def ->
  t -> ('req -> 'res) -> unit = fun req res ->
  let server = server_request req in
  let encode_response = Framing.create_method_frame res in

  let handler t (f: 'req -> 'res) req =
    f req
    |> encode_response ~channel_no:t.channel_no
    |> Stream.send t.send_stream
  in

  fun t f ->
    server t (handler t f)

(** De-register existing service registration.
    This function is not thread safe!
*)
let deregister_service: _ Protocol.Spec.def -> t -> unit = fun def t ->
  Message_table.remove t.services def.message_id

(** Expect only one request from the server and deregister
    the service handler immediately after receiving the request.
    The function will block until a request has been received and
    return the request/response pair *)
let server_request_response_oneshot:
  ('req, _, _, _, _, _, _, _, _, _) Protocol.Spec.def ->
  ('res, _, _, _, _, _, _, _, _, _) Protocol.Spec.def ->
  t -> ('req -> 'res) -> ('req * 'res) = fun req res ->
  let server_request_response = server_request_response req res in
  let handler t f promise request =
    deregister_service req t;
    let response = f request in
    Promise.resolve_ok promise (request, response);
    response
  in
  fun t f ->
    let p, u = Promise.create () in
    server_request_response t (handler t f u);
    Promise.await p
