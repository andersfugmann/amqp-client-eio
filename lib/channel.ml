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

type content = Spec.Basic.Content.t * Cstruct.t list
type deliver = Spec.Basic.Deliver.t * content
type 'a acks = (int * 'a Promise.u) Mlist.t

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

type consumers = (consumer_tag, deliver Stream.t) Hashtbl.t

(** Channel no can be removed, if we change the send to have send actually add the frame header and frame end. *)
type 'a t = {
  channel_no: int;
  service: Service.t;
  frame_max: int;
  publish: Cstruct.t list -> 'a;
  command_stream: 'a command Stream.t; (** Communicate commands *)
  set_flow: bool -> unit;
}

let register_consumer t ~id ~receive_stream =
  let p, u = Promise.create () in
  Stream.send t.command_stream (Register_consumer { id; stream = receive_stream; promise = u });
  Promise.await p

let deregister_consumer t ~consumer_tag =
  let p, u = Promise.create () in
  Stream.send t.command_stream (Deregister_consumer { consumer_tag; promise = u });
  Promise.await p

let publish_confirm: type a. flow_ready:(unit -> unit) -> command_stream:a command Stream.t -> a confirm -> Cstruct.t list -> a = fun ~flow_ready ~command_stream ->
  let send message =
    flow_ready ();
    Stream.send command_stream message
  in
  function
  | With_confirm ->
    fun data ->
      let p, u = Promise.create () in
      send (Publish { data; ack=Some u });
      Promise.await p
  | No_confirm ->
    fun data ->
      send (Publish { data; ack=None })


let publish: type a. a t -> Cstruct.t list -> a = fun t -> t.publish

let rec handle_commands t ~next_message_id ~next_consumer_id ~waiters ~consumers ~acks ~send_stream =
  let next_message_id, next_consumer_id =
    match Stream.receive t.command_stream with
    | Send_request { message; replies }  ->
      Queue.push replies waiters;
      List.iter ~f:(Stream.send send_stream) message;
      next_message_id, next_consumer_id
    | Publish { data; ack }  ->
      List.iter ~f:(Stream.send send_stream) data;
      begin
        match ack with
        | None ->
          next_message_id + 1, next_consumer_id
        | Some promise ->
          Mlist.append acks (next_message_id, promise);
          next_message_id + 1, next_consumer_id
      end
    | Register_consumer { id; stream; promise } ->
      let consumer_tag = Printf.sprintf "%s.%d" id next_consumer_id in
      Hashtbl.add consumers consumer_tag stream;
      Eio.Promise.resolve_ok promise consumer_tag;
      next_message_id, next_consumer_id + 1
    | Deregister_consumer { consumer_tag; promise } ->
      match Hashtbl.find_opt consumers  consumer_tag with
      | Some stream ->
        Stream.close stream (Closed "Closed by user");
        Hashtbl.remove consumers consumer_tag;
        Eio.Promise.resolve_ok promise ();
        next_message_id, next_consumer_id
      | None ->
        Eio.Promise.resolve_error promise (Failure "Consumer not found");
        next_message_id, next_consumer_id

  in
  handle_commands t ~next_message_id ~next_consumer_id ~waiters ~consumers ~acks ~send_stream

exception Channel_closed of Spec.Channel.Close.t

type flow_state = { mutable flow: bool; mutable blocked: bool }
let set_flow monitor state = function
  | v when state.flow = v -> ()
  | v -> Utils.Monitor.update monitor (fun state -> state.flow <- v) state

let set_blocked monitor state = function
  | v when state.blocked = v -> ()
  | v -> Utils.Monitor.update monitor (fun state -> state.blocked <- v) state

let flow_ready monitor state =
  Utils.Monitor.wait monitor ~predicate:(fun { flow; blocked } -> flow && not blocked) state


(** Initiate graceful shutdown *)
let shutdown t ~waiters ~consumers ~acks ~receive_stream reason =
  Eio.traceln ~__POS__ "Shutting down channel %d" t.channel_no;
  Stream.close t.command_stream reason;

  (* Cancel all acks *)
  Mlist.take_while ~pred:(fun _ -> true) acks |> List.iter ~f:(fun (_, promise) -> Promise.resolve_exn promise reason);
  Queue.iter (fun waiter -> List.iter ~f:(fun { promise; _ } -> Promise.resolve_exn promise reason) waiter) waiters;
  Queue.clear waiters;
  Hashtbl.iter (fun _key stream -> Stream.close stream reason) consumers;
  Stream.close receive_stream reason;
  ()

let handle_return: _ t -> Spec.Basic.Return.t -> Spec.Basic.Content.t -> Cstruct.t list -> unit = fun t return content body ->
  ignore (t, return, content, body);
  failwith "Not implemented"

let handle_deliver: consumers -> Spec.Basic.Deliver.t -> Spec.Basic.Content.t -> Cstruct.t list -> unit = fun consumers deliver content body ->
  match Hashtbl.find_opt consumers deliver.consumer_tag with
  | Some stream -> Stream.send stream (deliver, (content, body))
  | None -> failwith "Could not find consumer for delivered message"

let handle_confirm: type a. a confirm -> a acks -> _ = fun confirm acks ->
  let confirm: a acks -> delivery_tag:delivery_tag -> multiple:bool -> [ `Ok | `Rejected ] -> unit =
    match confirm with
    | No_confirm -> fun _acks ~delivery_tag:_ ~multiple:_  _ -> ()
    | With_confirm -> fun acks ~delivery_tag ~multiple result ->
      match multiple with
      | true ->
        let acks = Mlist.take_while ~pred:(fun (dt, _) -> dt <= delivery_tag) acks in
        List.iter ~f:(fun (_, promise) -> Eio.Promise.resolve_ok promise result) acks;
        ()
      | false ->
        match Mlist.take ~pred:(fun (dt, _) -> dt = delivery_tag) acks with
        | None -> failwith "Confirm recieved for non-existing message"
        | Some (_, promise) ->
          Eio.Promise.resolve_ok promise result
  in
  function
  | `Ack Spec.Basic.Ack.{ delivery_tag; multiple; } ->
    confirm acks ~delivery_tag ~multiple `Ok
  | `Nack Spec.Basic.Nack.{ delivery_tag; multiple; requeue = false } ->
    confirm acks ~delivery_tag ~multiple `Rejected
  | `Nack Spec.Basic.Nack.{ requeue = true; _ } ->
    failwith "Cannot requeue already sent messages"

let handle_ack confirm_f ack = confirm_f (`Ack ack)
let handle_nack confirm_f nack = confirm_f (`Nack nack)
let handle_close _t _ = failwith "Close Not Implemented"
let handle_flow set_flow Spec.Channel.Flow.{ active } =
  set_flow active;
  Spec.Channel.Flow_ok.{ active; }

let handle_message t data = function
  | Frame_type.Method -> Service.handle_method t.service data
  | Frame_type.Content_header -> failwith "Stray content header"
  | Frame_type.Content_body -> failwith "Stray content body"
  | Frame_type.Heartbeat -> failwith "Per channel heartbeats not expected"

let consume_messages ~receive_stream t =
  let rec loop () =
    let frame_type, data = Stream.receive receive_stream in
    handle_message t data frame_type;
    loop ()
  in
  loop ()


type consumer_state = Full | Empty
let handle_consumer_queue ~service ~flow_queue =
  let module Consumer_set = Set.Make(String) in
  let set_flow ~active =
    let Spec.Channel.Flow_ok.{ active=active' } = Spec.Channel.Flow.client_request service ~active () in
    assert (active' = active)
  in
  let rec loop flow_state blocked_queues =
    match Stream.receive flow_queue with
    | Full, consumer_name ->
      if not flow_state; then set_flow ~active:false;
      let blocked_queues = Consumer_set.add consumer_name blocked_queues in
      loop false blocked_queues
    | Empty, consumer_name ->
      let blocked_queues = Consumer_set.remove consumer_name blocked_queues in
      if (Consumer_set.is_empty blocked_queues && flow_state) then set_flow ~active:true;
      let flow_state = Consumer_set.is_empty blocked_queues in
      loop flow_state blocked_queues
  in
  loop true Consumer_set.empty



(* This should monitor all consumer queues, and post a message for
   state changes every time the queue changes state.


   Each time a consumer is created we start a fiber to flip between queue full and queue empty.
   We send a message (empty|full, consumer_name) to a fiber.
   The fiber should keep channel flow state and onblock if blocked and now existing consumers are blocked.Amqp_client_eio
   We should just keep a set of consumers.
*)


let monitor_queue_length ~service ~receive_stream =
  let set_flow ~active =
    let Spec.Channel.Flow_ok.{ active=active' } = Spec.Channel.Flow.client_request service ~active () in
    assert (active' = active)
  in

  let rec loop () =
    Stream.wait_full receive_stream;
    set_flow ~active:false;
    Stream.wait_empty receive_stream;
    set_flow ~active:true;
    loop ()
  in
  loop ()


let init: type a. sw:Eio.Switch.t -> Connection.t -> a confirm -> a t = fun ~sw connection confirm_type ->
  let receive_stream = Stream.create () in

  let flow_monitor = Monitor.init () in
  let flow_state = { flow = true; blocked = false} in
  let set_flow = set_flow flow_monitor flow_state in
  let set_blocked = set_blocked flow_monitor flow_state in
  let flow_ready () = flow_ready flow_monitor flow_state in

  (* Request channel number and register the receiving stream for same *)
  let Connection.{ channel_no; send_stream; frame_max } =
    Connection.register_channel connection ~set_blocked ~receive_stream
  in

  let service = Service.init ~send_stream ~receive_stream ~channel_no () in

  let command_stream = Stream.create ~capacity:5 () in
  let t = {
    channel_no;
    command_stream;
    frame_max;
    service;
    publish = publish_confirm ~flow_ready ~command_stream confirm_type;
    set_flow;
  }
  in


  let waiters = Queue.create () in
  let consumers = Hashtbl.create 7 in
  let acks = Mlist.create () in
  Eio.Fiber.fork_sub
    ~sw
    ~on_error:(fun exn -> Eio.traceln ~__POS__ "Channel exited: %s" (Printexc.to_string exn); shutdown t ~waiters ~consumers ~acks ~receive_stream exn)
    (fun sw ->
       let handle_confirm = handle_confirm confirm_type acks in
       (* Setup the service *)
       Spec.Basic.Deliver.server_request service (handle_deliver consumers);
       Spec.Basic.Ack.server_request service (handle_ack handle_confirm);
       Spec.Basic.Nack.server_request service (handle_nack handle_confirm);
       Spec.Channel.Close.server_request service (handle_close t);
       Spec.Channel.Flow.server_request service (handle_flow set_flow);
       Spec.Basic.Return.server_request service (handle_return t);

       (* Start handling messages before channel open *)
       Eio.Fiber.fork ~sw (fun () -> consume_messages ~receive_stream t);

       Spec.Channel.Open.client_request service ();

       (* Enable message confirms *)
       let () = match confirm_type with
         | With_confirm -> Spec.Confirm.Select.(client_request service ~nowait:false ())
         | No_confirm -> ()
       in

       Eio.Fiber.fork ~sw (fun () -> try handle_commands t ~next_message_id:1 ~next_consumer_id:1 ~waiters ~consumers ~acks ~send_stream with _ -> ());
    );
  t
