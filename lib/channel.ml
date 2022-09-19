(** Channel. Functions for dispatching messages *)
open StdLabels
module Queue = Stdlib.Queue
open Types
open Utils

type _ confirm =
  | No_confirm: unit confirm
  | With_confirm: [ `Ok | `Rejected ] confirm

let no_confirm = No_confirm
let with_confirm = With_confirm

exception Closed of string

type consumer_tag = shortstr
type delivery_tag = int
type nonrec string = string
type nonrec int = int

type message_reply = { message_id: message_id;
                       has_content: bool;
                       promise: (Cstruct.t * Framing.content option) Promise.u;
                     }

type content = Spec.Basic.Content.t * string
type deliver = Spec.Basic.Deliver.t * content

type 'a command =
  | Send_request of { message: Cstruct.t list; replies: message_reply list }
  (** Send a request, and wait for replies of the listed message types *)
  | Publish of { data: Cstruct.t list; ack: 'a Promise.u option }
  (** Publish a message. When publisher confirms are enabled, the promise will be fulfilled when ack | nack is received.
      If not in ack mode, the given promise will be fullfilled immediately. Alternatively client can use Send_request instead *)
  | Register_consumer of { id: string; stream: deliver Stream.t; promise: consumer_tag Promise.u; }
  (** Register a consumer. *)
  | Deregister_consumer of { consumer_tag: consumer_tag;  promise: unit Promise.u; }
  (** De-register a consumer. *)

(* TODO: Clean this up. Many of the fields do not need to be carried around. *)
type 'a t = {
  channel_no: int;
  service: Service.t;
  frame_max: int;
  ok: 'a;
  confirms_enabled: bool; (* This is useless I think *)
  receive_stream: (Types.Frame_type.t * Cstruct.t) Stream.t; (** Receiving messages *)
  send_stream: Cstruct.t Stream.t; (** Message send stream *)
  command_stream: 'a command Stream.t; (** Communicate commands *)
  waiters: (message_reply list) Queue.t; (** Users waiting for Replies. This should be a map. *)
  mutable acks: (int * 'a Promise.u) Mlist.t;
  consumers: (consumer_tag, deliver Stream.t) Hashtbl.t;
  mutable next_message_id: int;
  flow: Binary_semaphore.t;
  mutable next_consumer_id: int;
}

let register_consumer t ~id ~receive_stream =
  let p, u = Promise.create () in
  Stream.send t.command_stream (Register_consumer { id; stream = receive_stream; promise = u });
  Promise.await p

let deregister_consumer t ~consumer_tag =
  let p, u = Promise.create () in
  Stream.send t.command_stream (Deregister_consumer { consumer_tag; promise = u });
  Promise.await p

let publish: type a. a t -> Cstruct.t list -> a = fun t data ->
  let send t message =
    Binary_semaphore.wait t.flow true;
    Stream.send t.command_stream message
  in

  match t.confirms_enabled with
  | true ->
    let p, u = Promise.create () in
    send t (Publish { data; ack=Some u });
    Promise.await p
  | false ->
    send t (Publish { data; ack=None });
    t.ok

let rec handle_commands t =
  let () =
    match Stream.receive t.command_stream with
    | Send_request { message; replies }  ->
      Queue.push replies t.waiters;
      List.iter ~f:(Stream.send t.send_stream) message;
    | Publish { data; ack }  ->
      List.iter ~f:(Stream.send t.send_stream) data;
      begin
        match t.confirms_enabled, ack with
        | false, None -> ()
        | false, Some promise -> Eio.Promise.resolve_ok promise t.ok
        | true, None ->
          t.next_message_id <- t.next_message_id + 1;
          ()
        | true, Some promise ->
          Mlist.append t.acks (t.next_message_id, promise);
          t.next_message_id <- t.next_message_id + 1;
          ()
      end
    | Register_consumer { id; stream; promise } ->
      let consumer_tag = Printf.sprintf "%s.%d" id t.next_consumer_id in
      t.next_consumer_id <- t.next_consumer_id + 1;
      Hashtbl.add t.consumers consumer_tag stream;
      Eio.Promise.resolve_ok promise consumer_tag
    | Deregister_consumer { consumer_tag; promise } ->
      match Hashtbl.find_opt t.consumers  consumer_tag with
      | Some stream ->
        Stream.close stream (Closed "Closed by user");
        Hashtbl.remove t.consumers consumer_tag;
        Eio.Promise.resolve_ok promise ()
      | None ->
        Eio.Promise.resolve_error promise (Failure "Consumer not found")

  in
  handle_commands t

exception Channel_closed of Spec.Channel.Close.t

(** Initiate graceful shutdown *)
let shutdown t reason =
  Stream.close t.command_stream reason;

  (* Cancel all acks *)
  Mlist.take_while ~pred:(fun _ -> true) t.acks |> List.iter ~f:(fun (_, promise) -> Promise.resolve_exn promise reason);
  Queue.iter (fun waiter -> List.iter ~f:(fun { promise; _ } -> Promise.resolve_exn promise reason) waiter) t.waiters;
  Queue.clear t.waiters;
  Hashtbl.iter (fun _key stream -> Stream.close stream reason) t.consumers;
  ()

let handle_return: _ t -> Spec.Basic.Return.t -> Spec.Basic.Content.t -> Cstruct.t list -> unit = fun t return content body ->
  ignore (t, return, content, body);
  failwith "Not implemented"

let handle_deliver: _ t -> Spec.Basic.Deliver.t -> Spec.Basic.Content.t -> Cstruct.t list -> unit = fun t deliver content body ->
  let data = Bytes.create (Cstruct.lenv body) in
  let (_: int) =
    List.fold_left ~init:0 ~f:(fun offset buf ->
      let length = Cstruct.length buf in
      Cstruct.blit_to_bytes buf 0 data offset length; offset + length
    ) body
  in

  match Hashtbl.find_opt t.consumers deliver.consumer_tag with
  | Some stream -> Stream.send stream (deliver, (content, Bytes.unsafe_to_string data))
  | None -> failwith "Could not find consumer for delivered message"

let handle_confirm: type a. a confirm -> a t -> _ = fun confirm t ->
  let confirm: a t -> delivery_tag:delivery_tag -> multiple:bool -> [ `Ok | `Rejected ] -> unit =
    match confirm with
    | No_confirm -> fun _t ~delivery_tag:_ ~multiple:_  _ -> ()
    | With_confirm -> fun t ~delivery_tag ~multiple result ->
      match multiple with
      | true ->
        let acks = Mlist.take_while ~pred:(fun (dt, _) -> dt <= delivery_tag) t.acks in
        List.iter ~f:(fun (_, promise) -> Eio.Promise.resolve_ok promise result) acks;
        ()
      | false ->
        match Mlist.take ~pred:(fun (dt, _) -> dt = delivery_tag) t.acks with
        | None -> failwith "Confirm recieved for non-existing message"
        | Some (_, promise) ->
          Eio.Promise.resolve_ok promise result
  in
  function
  | `Ack Spec.Basic.Ack.{ delivery_tag; multiple; } ->
    confirm t ~delivery_tag ~multiple `Ok
  | `Nack Spec.Basic.Nack.{ delivery_tag; multiple; requeue = false } ->
    confirm t ~delivery_tag ~multiple `Rejected
  | `Nack Spec.Basic.Nack.{ requeue = true; _ } ->
    failwith "Cannot requeue already sent messages"

let handle_ack confirm_f ack = confirm_f (`Ack ack)
let handle_nack confirm_f nack = confirm_f (`Nack nack)
let handle_close _t _ = failwith "Close Not Implemented"
let handle_flow _t _ = failwith "Flow Not Implemented"


let handle_message t data = function
  | Frame_type.Method -> Service.handle_method t.service data
  | Frame_type.Content_header -> failwith "Stray content header"
  | Frame_type.Content_body -> failwith "Stray content body"
  | Frame_type.Heartbeat -> failwith "Per channel heartbeats not expected"

let rec consume_messages t =
  let frame_type, data = Stream.receive t.receive_stream in
  handle_message t data frame_type;
  consume_messages t

let init: type a. sw:Eio.Switch.t -> Connection.t -> a confirm -> a t = fun ~sw connection confirm_type ->
  let has_confirm, ok =
    let get_confirm: type a. a confirm -> bool * a = function
      | No_confirm -> false, ()
      | With_confirm -> true, `Ok
    in
    get_confirm confirm_type
  in

  let flow = Binary_semaphore.create true in (* Allow flow *)
  let receive_stream = Stream.create () in

  (* Request channel number and register the receiving stream for same *)
  (* TODO: Implemente flow handling *)
  let Connection.{ channel_no; send_stream; frame_max } = Connection.register_channel connection ~flow:(fun ~blocked -> Binary_semaphore.set flow (not blocked)) ~receive_stream in

  let service = Service.init ~send_stream ~receive_stream ~channel_no () in

  let t = {
    confirms_enabled = has_confirm;
    channel_no;
    receive_stream;
    send_stream;
    command_stream = Stream.create ~capacity:5 ();
    frame_max;
    service;
    ok;

    (* When we close a stream, we try to post a message. This is problematic if the consumer trues to close => deadlock. *)
    (* So how do we close a stream? *)
    acks = Mlist.create ();
    waiters = Queue.create ();
    consumers = Hashtbl.create 7;
    next_message_id = 1;
    flow;
    next_consumer_id = 0;
  }
  in

  Eio.Fiber.fork_sub
    ~sw
    ~on_error:(fun exn -> Eio.traceln ~__POS__ "Channel exited: %s" (Printexc.to_string exn); shutdown t exn)
    (fun sw ->

       let handle_confirm = handle_confirm confirm_type t in
       (* Setup the service *)
       Spec.Basic.Deliver.server_request service (handle_deliver t);
       Spec.Basic.Ack.server_request service (handle_ack handle_confirm);
       Spec.Basic.Nack.server_request service (handle_nack handle_confirm);
       Spec.Channel.Close.server_request service (handle_close t);
       Spec.Channel.Flow.server_request service (handle_flow t);
       Spec.Basic.Return.server_request service (handle_return t);

       (* Start handling messages before channel open *)
       Eio.Fiber.fork ~sw (fun () -> consume_messages t);

       Spec.Channel.Open.client_request service ();

       (* Enable message confirms *)
       if t.confirms_enabled then Spec.Confirm.Select.(client_request service ~nowait:false ());

       Eio.Fiber.fork ~sw (fun () -> try handle_commands t with _ -> ());
    );
  t
