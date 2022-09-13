(** Channel. Functions for dispatching messages *)
open StdLabels
module Queue = Stdlib.Queue
open Types
open Utils

type _ confirms =
  | No_confirm: [ `Ok ] confirms
  | With_confirm: [ `Ok | `Rejected ] confirms

let no_confirm = No_confirm
let with_confirm = With_confirm

type 'a confirm = 'a confirms
  constraint 'a = [> `Ok ]
  constraint 'a = [< `Ok | `Rejected ]

exception Closed of string

type consumer_tag = shortstr
type delivery_tag = int
type nonrec string = string
type nonrec int = int

type message_reply = { message_id: message_id;
                       has_content: bool;
                       promise: (Cstruct.t * Framing.content option) Promise.u;
                     }

module Message = struct
  type content = Spec.Basic.Content.t * Cstruct.t list

  type t =
    { delivery_tag : int;
      redelivered : bool;
      exchange : string;
      routing_key : string;
      content: content;
    }
end
type deliver = Spec.Basic.Deliver.t * Message.content

type 'a command =
  | Send_request of { message: Cstruct.t list; replies: message_reply list }
  (** Send a request, and wait for replies of the listed message types *)
  | Publish of { data: Cstruct.t list; ack: 'a Promise.u option }
  (** Publish a message. When publisher confirms are enabled, the promise will be fulfilled when ack | nack is received.
      If not in ack mode, the given promise will be fullfilled immediately. Alternatively client can use Send_request instead *)
  | Register_consumer of { consumer_tag: consumer_tag; stream: deliver Stream.t; promise: (unit, [ `Consumer_already_registered of string]) Result.t Promise.u; }
  (** Register a consumer. *)
  | Deregister_consumer of { consumer_tag: consumer_tag;  promise: unit Promise.u; }
  (** De-register  a consumer. *)

type 'a t = {
  connection: Connection.t;
  confirms_enabled: bool; (* This is useless I think *)
  channel_no: int;
  receive_stream: (Types.Frame_type.t * Cstruct.t) Stream.t; (** Receiving messages *)
  send_stream: Cstruct.t Stream.t; (** Message send stream *)
  command_stream: 'a command Stream.t; (** Communicate commands *)
  frame_max: int;
  service: Service.t;
  ok: 'a;
  mutable acks: (int * 'a Promise.u) Mlist.t;
  waiters: (message_reply list) Queue.t; (** Users waiting for Replies. This should be a map. *)
  consumers: (consumer_tag, deliver Stream.t) Hashtbl.t;
  mutable next_message_id: int;
  mutable flow: (unit Promise.t * unit Promise.u);
  mutable close_reason: exn option;
}

type 'a with_confirms = 'a confirm -> 'a t
  constraint 'a = [> `Ok ]
  constraint 'a = [< `Ok | `Rejected ]


let publish: type a. a t -> Cstruct.t list -> a = fun t data ->
  match t.confirms_enabled with
  | true ->
    let p, u = Promise.create () in
    Stream.send t.command_stream (Publish { data; ack=Some u });
    Promise.await p
  | false ->
    Stream.send t.command_stream (Publish { data; ack=None });
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
        | false, Some promise -> Eio.Promise.resolve_ok promise `Ok
        | true, None ->
          t.next_message_id <- t.next_message_id + 1;
          ()
        | true, Some promise ->
          Mlist.append t.acks (t.next_message_id, promise);
          t.next_message_id <- t.next_message_id + 1;
          ()
      end
    | Register_consumer { consumer_tag; stream; promise } ->
      begin
        match Hashtbl.mem t.consumers consumer_tag with
        | false ->  Hashtbl.add t.consumers consumer_tag stream
        | true -> Eio.Promise.resolve_ok promise (Error (`Consumer_already_registered consumer_tag))
      end
    | Deregister_consumer { consumer_tag; promise } ->
      Hashtbl.remove t.consumers consumer_tag;
      Eio.Promise.resolve_ok promise ()
  in
  handle_commands t

exception Channel_closed of Spec.Channel.Close.t

type 'a x = [< `Ok | `Rejected > `Ok ] as 'a

(** Initiate graceful shutdown *)
let shutdown t reason =
  t.close_reason <- Some reason;

  begin
    match Promise.is_resolved (fst t.flow) with
    | true -> ()
    | false -> Promise.resolve_exn (snd t.flow) reason
  end;

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

  match Hashtbl.find_opt t.consumers deliver.consumer_tag with
  | Some stream -> Stream.send stream (deliver, (content, body))
  | None -> failwith "Could not find consumer for delivered message"

let handle_confirm: type a. a confirms -> a t -> _ = fun confirm t ->
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
let handle_close _t _ = failwith "Not Implemented"
let handle_flow _t _ = failwith "Not Implemented"


let handle_message t data = function
  | Frame_type.Method -> Service.handle_method t.service data
  | Frame_type.Content_header -> failwith "Stray content header"
  | Frame_type.Content_body -> failwith "Stray content body"
  | Frame_type.Heartbeat -> failwith "Per channel heartbeats not expected"

let rec consume_messages t =
  let frame_type, data = Stream.receive t.receive_stream in
  handle_message t data frame_type;
  consume_messages t

let init: sw:Eio.Switch.t -> Connection.t -> 'a with_confirms = fun ~sw connection confirm_type ->
  let has_confirm =
    let get_confirm: type a. a confirms -> bool = function
      | No_confirm -> false
      | With_confirm -> true
    in
    get_confirm confirm_type
  in

  let receive_stream = Stream.create () in
  (* Request channel number and register the receiving stream for same *)
  let Connection.{ channel_no; send_stream; frame_max } = Connection.register_channel connection receive_stream in

  let service = Service.init ~send_stream ~receive_stream ~channel_no () in

  let t = {
    connection;
    confirms_enabled = has_confirm;
    channel_no;
    receive_stream;
    send_stream;
    command_stream = Stream.create ~capacity:1 ();
    frame_max;
    service;
    ok = `Ok;
    (* When we close a stream, we try to post a message. This is problematic if the consumer trues to close => deadlock. *)
    (* So how do we close a stream? *)
    acks = Mlist.create ();
    waiters = Queue.create ();
    consumers = Hashtbl.create 7;
    next_message_id = 1;
    flow = Promise.create ();
    close_reason = None;
  }
  in

  Eio.Fiber.fork_sub
    ~sw
    ~on_error:(fun exn -> Printf.printf "Channel exited: %s\n%!" (Printexc.to_string exn); shutdown t exn)
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
