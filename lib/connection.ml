(** AMQP client connection *)

(* Connection may be closed in three ways.

   Client initiated:

   1a. A message is send to the server requesting close
   1b. The send stream is closed with 'closed by user'
   2. The send steam is detected as closed and the send part of the flow is closed
   3. The receive stream receives forwards close_ok to the channel 0 handler
   4. The receive stream is closed by the server. The receive stream is closed and the thread exits.
   5. The channel0 handler receives the close_ok, and closes all channels (And aborts all waiters)
   6. the switch ends

   Server initiated:
   1. The receive thread forward Close message to the channel handler.
   2. The receive channel get tcp closed and closes the receive thread (and flushes)
   3a. The channel handler posts ok to the send thread
   3b. The send channel is closed (with 'Closed by server')
   3. Channel0 handler closes the receive channel
   4. The channel0 notifies all waiters and raises 'Closed by server'
   Q: Where do we log the reason?

   Tcp error on the receive stream:
   1. The receive thread receives error and closes the receive stream
   2. The frame demultiplexer closes the channel0 stream
   3. The channel0 handler raises (or closes)
   3. All waiters are closed

   Tcp erro on the sender stream:
   1. The sender stream encounters a tcp error.
   2. The sender stream closes the send stream
   3. The sender stream raises
   4. All waiters are closed

   A blocked connection should not publish new messages.
   Messages are published by channels, so the message should be relayed to all channels.
   If we block the sender, we cannot close the connection, thats bad.
   So we should just disable publish in all channels. Messages are published by call to single function.

*)


open StdLabels
module Queue = Stdlib.Queue
open Utils

exception Closed of string

let version = "0.0.1"
let max_frame_size = 131072
let max_channels = 2047

let printf fmt = Printf.printf fmt

module Credentials = struct
  type username = string
  type password = string
  type t = { username: string; password: string; mechanism: string }
  let make ~username ~password = { username; password; mechanism = "PLAIN" }
  let default = make ~username:"guest" ~password:"guest"
end

type channel_stream = (Types.Frame_type.t * Cstruct.t) Stream.t
type send_stream = Cstruct.t Stream.t
type channel_info = { channel_no: int; send_stream: send_stream; frame_max: int }
type flow = blocked:bool -> unit

type command = Register_channel of { receive_stream: channel_stream; flow: flow; promise: channel_info Promise.u }
             | Deregister_channel of { channel_no: int; promise: unit Promise.u }
             | Close

type t = {
  command_stream: command Stream.t;
}

let handle_register_channel ~channels ~send_stream ~max_channels ~next_channel ~frame_max ~receive_stream ~flow promise =
  (* Find the next free channel no *)
  let rec find_next_free_channel idx = function
    | 0 -> failwith "No available channels"
    | n -> match channels.(idx) with
      | None -> idx
      | Some _ -> find_next_free_channel ((idx + 1) mod max_channels) (n - 1)
  in
  let channel_no = find_next_free_channel next_channel max_channels in
  (* Store back the indicator for next possible free channel number *)
  channels.(channel_no) <- Some (receive_stream, flow);
  Promise.resolve_ok promise { channel_no; send_stream; frame_max };
  (channel_no + 1) mod max_channels

let handle_deregister_channel channels channel_no promise =
  channels.(channel_no) <- None;
  Promise.resolve_ok promise ()

let shutdown ~blocked ~send_stream ~channels reason =
  (* Should we close the socket now... Yes! *)
  Promise.resolve_exn blocked reason;
  Stream.close send_stream reason;
  Array.iter ~f:(function Some channel -> Stream.close channel reason | None -> ()) channels;
  ()

(** Command handler. User commands (and channel commands) are serialized though this. *)
let command_handler ~command_stream ~service ~send_stream ~channels ~max_channels ~frame_max =
  ignore service; (* We should use the service to handle incoming requests (e.g. close and block), and to send commands to the server *)
  let rec loop next_channel =
    let next_channel = match Stream.receive command_stream with
      | Register_channel { receive_stream; flow; promise } ->
        handle_register_channel
          ~channels ~send_stream ~max_channels ~frame_max ~next_channel ~flow ~receive_stream promise
      | Deregister_channel { channel_no; promise } ->
        handle_deregister_channel channels channel_no promise;
        next_channel
      | _ -> failwith "unsupported command"
    in
    loop next_channel
  in
  try
    loop 1
  with
  | Closed _ -> ()

let send_command t command =
  Stream.send t.command_stream command

(**** User callable functions *****)

(** Register consumption for a free channel *)
let register_channel t ~flow ~receive_stream =
  let pt, pu = Promise.create ~label:"Register channel" () in
  let command = Register_channel { receive_stream; flow; promise = pu } in
  send_command t command;
  Promise.await pt

(** Deregister consumption for a given channel *)
let deregister_channel t ~channel_no =
  let pt, pu = Promise.create ~label:"Register channel" () in
  let command = Deregister_channel { channel_no; promise = pu } in
  send_command t command;
  Promise.await pt

let handle_start_message ~id ~credentials Spec.Connection.Start.{ version_major; version_minor; server_properties; mechanisms; locales } =
  let print_map map =
    List.iter ~f:(fun (k, v) -> printf " %s = " k; Types.print_type " " v; printf "\n";) map
  in
  printf "Version: %d.%d\n" version_major version_minor;
  printf "Server properties:\n";
  print_map server_properties;
  printf "Mechanisms: %s\n" mechanisms;
  printf "Locales: %s\n" locales;

  (* Produce a reply *)
  let mechanism = credentials.Credentials.mechanism in
  let response = "\x00" ^ credentials.Credentials.username ^ "\x00" ^ credentials.Credentials.password in
  let locale = String.split_on_char ~sep:';' locales |> List.hd in
  let client_id = Printf.sprintf "%s.%s.%d.%s" id (Unix.gethostname ()) (Unix.getpid ()) (Sys.executable_name)  in
  let platform = Printf.sprintf "%s/Ocaml %s (%s)" Sys.os_type Sys.ocaml_version (match Sys.backend_type with | Sys.Bytecode -> "bytecode" | Sys.Native -> "native code" | Sys.Other s -> s) in
  let client_properties =
    let open Types in
    [
      "platform", VLongstr platform;
      "library", VLongstr "amqp-client-eio";
      "version", VLongstr version;
      "client id", VLongstr client_id;
      "information", VLongstr "Distributed under BSD-3 revised licence";
      "capabilities", VTable [
        "publisher_confirms", VBoolean true;
        "exchange_exchange_bindings", VBoolean true;
        "basic.nack", VBoolean true;
        "consumer_cancel_notify", VBoolean true;
        "connection.blocked", VBoolean true;
        "consumer_priorities", VBoolean true;
        "authentication_failure_close", VBoolean true;
        "per_consumer_qos", VBoolean true;
        "direct_reply_to", VBoolean true; (* Please don't ever use direct reply to though *)
      ]
  ]
  in
  Spec.Connection.Start_ok.{
    client_properties;
    mechanism;
    response;
    locale;
  }


let handle_tune_message ?heartbeat ~max_frame_size Spec.Connection.Tune.{ channel_max; frame_max; heartbeat = tune_heartbeat } =
  let channel_max = Int.max max_channels channel_max in
  let frame_max = Int.max max_frame_size frame_max in
  let heartbeat = match heartbeat with
    | None -> tune_heartbeat
    | Some heartbeat -> (Int.min tune_heartbeat !heartbeat)
  in
  Spec.Connection.Tune_ok.{ channel_max; frame_max; heartbeat }

(** Handle connection messages. Create a more generic system for handling incoming messages. *)
let handle_connection_messages service receive_stream _command_stream =
  let rec loop () =
    let () =
      match Stream.receive receive_stream with
      | Types.Frame_type.Heartbeat, _data ->
        (* TODO: We ought to record the timestamp of the last received message, and have the heartbeat thread
           check if we received any messages. Its somewhat annoying that we need mutability though.
        *)
        ()
      | Content_header, _data
      | Content_body, _data ->
        failwith "Channel 0 should never receive content messages"
      | Method, data ->
        Service.handle_method service data
    in
    loop ()
  (*
      begin
        let message_id, data = Framing.decode_method_header data in
        match message_id with
        | message_id when Types.Message_id.equal message_id Spec.Connection.Blocked.message_id ->
          () (* Should send a command to block the send stream *)
        | message_id when Types.Message_id.equal message_id Spec.Connection.Unblocked.message_id ->
          ()
        | message_id when Types.Message_id.equal message_id close_id ->
          let close_connection message =
            let reason =
              match message with
              | Spec.Connection.Close.{ reply_code; reply_text; class_id = 0; method_id = 0 } ->
                Printf.sprintf "%d: %s" reply_code reply_text
              | { reply_code; reply_text; class_id; method_id } ->
                Printf.sprintf "%d: Server terminated connection due to protocol failure. Text: %s. Offending message id: (%d, %d)" reply_code reply_text class_id method_id;
            in
            Stream.close command_stream (Closed reason);
          in
          close_f ~stream:send_stream ~channel_no:0 data (fun mk close_reason -> close_connection close_reason; mk);
     ()
    | _ -> failwith "Unknown message on channel 0"
     end;
     loop ()
  *)
  in
  try
    loop ()
  with exn ->
    Eio.traceln "Handle connection messages terminated with exn: %s" (Printexc.to_string exn);
    raise exn

(** Receive messages.
    If a TCP error is encountered the channel0 stream is closed and the function exits
    If the receive stream is closed, the receive flow is closed and the function exits
*)
let receive_messages ~set_close_reason flow channels =
  let rec loop () =
    let (frame_header, data) = Framing.read_frame flow in
    let () =
      match channels.(frame_header.channel) with
      | None -> (* Do some error handling of the message - we dont know it *)
        failwith_f "Data received on non-existing channel: %d" frame_header.channel
      | Some (channel, _flow) ->
        Stream.send channel (frame_header.frame_type, data)
    in
    loop ()
  in
  try
    loop ()
  with
  | End_of_file ->
    let exn = set_close_reason (Closed "Connection terminated") in
    Stream.close (Option.get channels.(0) |> fst) exn
  | Closed _ as exn ->
    Eio.Flow.shutdown flow `Receive;
    failwith_f "Channel closed with reason: %s" (Printexc.to_string exn)

let send_messages ~set_close_reason flow send_stream =
  Framing.write_protocol_header flow;
  (* We should handle exceptions here! *)
  let rec loop () =
    let frame = Stream.receive send_stream in
    Framing.write_data flow frame;
    loop ()
  in
  try
    loop ()
  with
  | End_of_file ->
    let exn = set_close_reason (Closed "Network connection terminated") in
    Stream.close send_stream exn
  | Closed _ as exn ->
    Eio.traceln "Send stream closed with exn: %s\n%!" (Printexc.to_string exn);
    Eio.Flow.shutdown flow `Send

(** Continuously send a heartbeat frame every [freq] / 2 *)
let send_heartbeat ~clock stream freq =
  (* Should verify that there has been a message from the server withing [freq] seconds, or terminate the connection if not. *)
  let freq = Float.of_int freq /. 2.0 in
  let rec loop () =
    Eio.Time.sleep clock freq;
    (* Only send if the stream is empty. We could optimize this to
       record last send, and only send when idle.
    *)
    if Stream.is_empty stream; then
      Stream.send stream Framing.heartbeat_frame;

    loop ()
  in
  try
    loop ()
  with
  | exn -> Eio.traceln "Heartbeat thread terminated with exn: %s\n%!" (Printexc.to_string exn);
    ()

module Blocked = struct
  type t = { mutable blocked: bool; mutex: Eio.Mutex.t; condition: Eio.Condition.t }

  let init () = { blocked = false; mutex = Eio.Mutex.create (); condition = Eio.Condition.create () }
  let block t = t.blocked <- true
  let unblock t =
    Eio.Mutex.lock t.mutex;
    t.blocked <- false;
    Eio.Condition.broadcast t.condition;
    Eio.Mutex.unlock t.mutex

  let rec wait_unblocked t =
    match t.blocked with
    | false -> ()
    | true ->
      Eio.Mutex.lock t.mutex;
      match t.blocked with
      | false ->
        Eio.Mutex.unlock t.mutex
      | true ->
        Eio.Condition.await t.condition t.mutex;
        Eio.Mutex.unlock t.mutex;
        wait_unblocked t
end




let handle_blocked channels Spec.Connection.Blocked.{ reason } =
  Eio.traceln "Connection blocked: %s" reason;
  Array.iter ~f:(function Some (_, flow) -> flow ~blocked:true | None -> ()) channels

let handle_unblocked channels () =
  Array.iter ~f:(function Some (_, flow) -> flow ~blocked:false | None -> ()) channels

let handle_close set_close_reason Spec.Connection.Close.{ reply_code; reply_text; class_id; method_id } =
  let reason = Printf.sprintf "Connection closed by server. %d: %s. (%d, %d)" reply_code reply_text class_id method_id in
  let _ = set_close_reason (Closed reason) in
  Eio.traceln "%s" reason

let set_close_reason, get_close_reason =
  let close_reason = ref None in
  (fun exn -> match !close_reason with Some exn -> exn | None -> close_reason := (Some exn); exn),
  (fun () -> !close_reason)

(** Create a channel to amqp *)
let init ~sw ~env ~id ?(virtual_host="/") ?heartbeat ?(max_frame_size=max_frame_size) ?(max_stream_length=5) ?(credentials=Credentials.default) ?(port=5672) host =
  let command_stream = Stream.create ~capacity:1 () in
  Eio.Fiber.fork_sub
    ~sw
    ~on_error:(fun exn -> Eio.traceln "Connection closed: %s\n%!" (Printexc.to_string exn))
    (fun sw ->
       let clock = Eio.Stdenv.clock env in

       let addr = Eio_unix.Ipaddr.of_unix (Unix.inet_addr_of_string host) in
       let net : #Eio.Net.t = Eio.Stdenv.net env in
       let flow = Eio.Net.connect ~sw net (`Tcp (addr, port)) in

       (* Create the streams *)
       let send_stream = Stream.create ~capacity:max_stream_length () in

       (* We just create max_channels. If the peer allows less, we waste a little memory *)
       let channels = Array.make max_channels None in

       (* Register channel 0 to handle messages *)
       let channel0_stream = Stream.create () in
       channels.(0) <- Some (channel0_stream, fun ~blocked:_ -> ());

       Eio.Fiber.fork ~sw (fun () -> send_messages ~set_close_reason flow send_stream);
       Eio.Fiber.fork ~sw (fun () -> receive_messages ~set_close_reason flow channels);

       let service = Service.init ~send_stream ~receive_stream:channel0_stream ~channel_no:0 () in
       Spec.Connection.Blocked.server_request service (handle_blocked channels);
       Spec.Connection.Unblocked.server_request service (handle_unblocked channels);
       (* We want to close the send stream after sending the message (i.e. last message sent) *)
       Spec.Connection.Close.server_request service (handle_close set_close_reason);

       Eio.Fiber.fork ~sw (fun () -> handle_connection_messages service channel0_stream command_stream);

       (* Now register the functions to handle messages *)
       let (_req, _res) =
         Spec.Connection.Start.server_request_oneshot service (handle_start_message ~id ~credentials)
       in
       Eio.traceln "Start Done";

       let (_, Spec.Connection.Tune_ok.{ channel_max; frame_max; heartbeat }) =
         Spec.Connection.Tune.server_request_oneshot service (handle_tune_message ?heartbeat ~max_frame_size)
       in
       Eio.traceln "Tune Done";
       Spec.Connection.Open.client_request service ~virtual_host ();

       (* Start sending heartbeats, and monitor for missing heartbeats *)
       Eio.Fiber.fork ~sw (fun () -> send_heartbeat ~clock send_stream heartbeat);

       Eio.Fiber.fork ~sw
         (fun () -> try
             command_handler ~command_stream ~send_stream ~service ~channels ~max_channels:channel_max ~frame_max
           with exn -> Eio.traceln "Command handler closed: %s: %s" (Printexc.to_string exn) (Printexc.get_backtrace ()); raise exn);

       ()
    );
  { command_stream }
