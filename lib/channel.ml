(** Channel. Functions for dispatching messages *)
open Types
open StdLabels
open Utils

type 'a confirms =
  | No_confirm: [ `Ok ] confirms
  | With_confirm: [ `Ok | `Rejected ] confirms

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

type deliver = { deliver : Spec.Basic.Deliver.t;
                 content_header: Framing.Content_header.t;
                 content_data: Cstruct.t;
                 body: Cstruct.t list;
               }

type 'a command =
  | Send_request of { message: Cstruct.t list; replies: message_reply list }
  (** Send a request, and wait for replies of the listed message types *)
  | Publish of { message: Cstruct.t list; ack: 'a Promise.u option }
  (** Publish a message. When publisher confirms are enabled, the promise will be fulfilled when ack | nack is received.
      If not in ack mode, the given promise will be fullfilled immediately. Alternatively client can use Send_request instead *)
  | Register_consumer of { consumer_tag: consumer_tag; stream: deliver Stream.t; promise: (unit, [ `Consumer_already_registered of string]) Result.t Promise.u; }
  (** Register a consumer. *)
  | Deregister_consumer of { consumer_tag: consumer_tag;  promise: unit Promise.u; }
  (** De-register  a consumer. *)
  (* constraint 'a = [> `Ok ]
     constraint 'a = [< `Ok | `Rejected ]*)

type 'a t = {
  connection: Connection.t;
  confirms_enabled: bool; (* This is useless I think *)
  channel_no: int;
  receive_stream: (Types.Frame_type.t * Cstruct.t) Stream.t; (** Receiving messages *)
  send_stream: Cstruct.t Stream.t; (** Message send stream *)
  command_stream: 'a command Stream.t; (** Communicate commands *)
  mutable acks: (int * 'a Promise.u) Mlist.t;
  waiters: (message_reply list) Queue.t; (** Users waiting for Replies. This should be a map. *)
  consumers: (consumer_tag, deliver Stream.t) Hashtbl.t;
  mutable next_message_id: int;
  mutable flow: (unit Promise.t * unit Promise.u);
  mutable close_reason: exn option;
} (* constraint 'a = [> `Ok ]
     constraint 'a = [< `Ok | `Rejected ] *)

type 'a with_confirms = 'a confirms -> 'a t
  (* constraint 'a = [> `Ok ]
     constraint 'a = [< `Ok | `Rejected ] *)


let rec handle_commands t =
  let () =
    match Stream.receive t.command_stream with
    | Send_request { message; replies }  ->
      Queue.push replies t.waiters;
      List.iter ~f:(Stream.send t.send_stream) message;
    | Publish { message; ack }  ->
      (* Only send when flow is enabled *)
      let () = Eio.Promise.await_exn (fst t.flow) in
      List.iter ~f:(Stream.send t.send_stream) message;
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

let handle_incoming_message: type a. a confirms -> a t -> (Types.Frame_type.t * Cstruct.t) -> unit = fun confirm ->
  let deliver_id, read_deliver =
    let open Protocol.Spec in
    let { message_id; spec; make; _ }, _content = Spec.Basic.Deliver.def in
    message_id, read spec make
  in
  let ack_id, read_ack =
    let open Protocol.Spec in
    let { message_id; spec; make; _ } = Spec.Basic.Ack.reply in
    message_id, read spec make
  in
  let nack_id, read_nack =
    let open Protocol.Spec in
    let { message_id; spec; make; _ } = Spec.Basic.Nack.reply in
    message_id,read spec make
  in
  let flow_id, flow_f = Framing.server_request_response Spec.Channel.Flow.reply in
  let close_id, close_f = Framing.server_request_response Spec.Channel.Close.reply in

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

  fun t -> function
  | (Types.Frame_type.Method, data) ->
    begin
      match Framing.decode_method_header data with
      | (message_id, data) when Types.Message_id.equal message_id deliver_id ->
        begin
          let deliver = read_deliver data 0 in
          let Framing.{ header; data; body } = Framing.read_content t.receive_stream in
          let message = { deliver; content_header = header; content_data = data; body } in
          match Hashtbl.find_opt t.consumers deliver.consumer_tag with
          | Some stream -> Stream.send stream message
          | None -> failwith "Could not find consumer for delivered message"
        end
      | (message_id, data) when Types.Message_id.equal message_id flow_id ->
        flow_f ~stream:t.send_stream ~channel_no:t.channel_no data @@ (fun init { active } ->
          let current = Eio.Promise.is_resolved (fst t.flow) in
          let () = match active, current with
            | true, true -> ()
            | false, false -> ()
            | true, false -> Eio.Promise.resolve_ok (snd t.flow) ()
            | false, true -> t.flow <- Eio.Promise.create ()
          in
          init ~active ()
        )
      | (message_id, data) when Types.Message_id.equal message_id close_id ->
        let exn = ref (failwith "error") in
        close_f ~stream:t.send_stream ~channel_no:t.channel_no data (fun init reason ->
          exn := Channel_closed reason;
          init ()
        );
        raise !exn
      | (message_id, data) when Types.Message_id.equal message_id ack_id ->
        let Spec.Basic.Ack.{ delivery_tag; multiple; } = read_ack data 0 in
        confirm t ~delivery_tag ~multiple `Ok
      | (message_id, data) when Types.Message_id.equal message_id nack_id ->
        let Spec.Basic.Nack.{ delivery_tag; multiple; requeue=_; } = read_nack data 0 in
        confirm t ~delivery_tag ~multiple `Rejected
      | (message_id, data) ->
        let message_replies = Queue.pop t.waiters in
        let handler = List.find ~f:(fun { message_id = message_id'; _ } -> Types.Message_id.equal message_id message_id') message_replies in
        (* If the handler is not found, we have a protocol violation *)
        let { message_id=_; has_content; promise } = handler in
        let content = match has_content with
          | false -> None
          | true -> Some (Framing.read_content t.receive_stream)
        in
        Eio.Promise.resolve_ok promise (data, content)
    end
  | ((Content_header | Content_body | Heartbeat), _data) -> failwith "Only message requests expected"

let rec handle_incoming_messages confirm t =
  let message = Stream.receive t.receive_stream in
  handle_incoming_message confirm t message;
  handle_incoming_messages confirm t


(** Initiate graceful shutdown *)
let shutdown t reason =
  t.close_reason <- Some reason;

  begin
    match Promise.is_resolved (fst t.flow) with
    | true -> ()
    | false -> Promise.resolve_exn (snd t.flow) reason
  end;

  Stream.close ~notify_consumers:false t.command_stream reason;

  (* Cancel all acks *)
  Mlist.take_while ~pred:(fun _ -> true) t.acks |> List.iter ~f:(fun (_, promise) -> Promise.resolve_exn promise reason);
  Queue.iter (fun waiter -> List.iter ~f:(fun { promise; _ } -> Promise.resolve_exn promise reason) waiter) t.waiters;
  Queue.clear t.waiters;
  Hashtbl.iter (fun _key stream -> Stream.close stream reason) t.consumers;
  ()

let init: sw:Eio.Switch.t -> Connection.t -> 'a with_confirms = fun ~sw connection confirm_type ->
  let has_confirm =
    let get_confirm: type a. a confirms -> bool = function
      | No_confirm -> false
      | With_confirm -> true
    in
    get_confirm confirm_type
  in

  let receive_stream = Stream.create () in
  let channel_no, send_stream = Connection.register_channel connection receive_stream in
  let t = {
    connection;
    confirms_enabled = has_confirm;
    channel_no;
    receive_stream;
    send_stream;
    command_stream = Stream.create ~max_size:1 ();
    (* When we close a stream, we try to post a message. This is problematic if the consumer trues to close => deadlock. *)
    (* So how do we close a stream? *)
    acks = Mlist.create ();
    waiters = Queue.create ();
    consumers = Hashtbl.create 7;
    next_message_id = 1;
    flow = Promise.create ();
    close_reason = None;
  } in

  (* We should use the standard framework to send and receive replies *)
  let channel_open = Framing.create_method_frame Spec.Channel.Open.def ~channel_no () in
  Stream.send t.send_stream channel_open;
  let frame_type, data = Stream.receive t.receive_stream in
  assert (Types.Frame_type.equal frame_type Types.Frame_type.Method);
  Framing.read_method Spec.Channel.Open_ok.def data;
  begin
    match t.confirms_enabled with
    | true ->
      let channel_open = Framing.create_method_frame_args Spec.Confirm.Select.def ~channel_no ~nowait:false () in
      Stream.send t.send_stream channel_open;
      let (_tpe, data) = Stream.receive t.receive_stream in
      let () = Framing.read_method Spec.Confirm.Select_ok.def data in
      ()
    | false -> ()
  end;

  Eio.Fiber.fork_sub
    ~sw
    ~on_error:(fun exn -> Printf.printf "Channel exited: %s\n%!" (Printexc.to_string exn); shutdown t exn)
    (fun sw ->
      Eio.Fiber.fork ~sw (fun () -> try handle_commands t with _ -> ());
      Eio.Fiber.fork ~sw (fun () -> handle_incoming_messages confirm_type t);
    );

  t
