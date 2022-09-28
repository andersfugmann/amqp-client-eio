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
let closed_by_user = Closed "Connection closed on user request"

let version = "0.0.1"
let max_frame_size = 131072
let max_channels = 2047

let printf fmt = Printf.printf fmt

module Credentials = struct
  type t = { username: string; password: string; mechanism: string }
  let make ~username ~password = { username; password; mechanism = "PLAIN" }
  let default = make ~username:"guest" ~password:"guest"
end

type channel_stream = (Types.Frame_type.t * Cstruct.t) Stream.t
type send_stream = Cstruct.t Stream.t
type channel_info = { channel_no: int; send_stream: send_stream; frame_max: int }

type command = Register_channel of { receive_stream: channel_stream; set_blocked:(bool -> unit); promise: channel_info Promise.u }
             | Deregister_channel of { channel_no: int; promise: unit Promise.u }
             | Close of string

type t = {
  command_stream: command Stream.t;
}

let handle_register_channel ~channels ~send_stream ~max_channels ~next_channel ~frame_max ~receive_stream ~set_blocked promise =
  (* Find the next free channel no *)
  let rec find_next_free_channel idx = function
    | 0 -> failwith "No available channels"
    | n -> match channels.(idx) with
      | None -> idx
      | Some _ -> find_next_free_channel ((idx + 1) mod max_channels) (n - 1)
  in
  let channel_no = find_next_free_channel next_channel max_channels in
  (* Store back the indicator for next possible free channel number *)
  channels.(channel_no) <- Some (receive_stream, set_blocked);
  Promise.resolve_ok promise { channel_no; send_stream; frame_max };
  (channel_no + 1) mod max_channels

let handle_deregister_channel channels channel_no promise =
  channels.(channel_no) <- None;
  Promise.resolve_ok promise ()

let shutdown ~send_stream ~channels ~command_stream exn =
  Eio.traceln ~__POS__ "Shutting down connection";
  Stream.close send_stream exn;
  Stream.close command_stream exn;
  Array.iter ~f:(function Some (channel, set_blocked) -> set_blocked false; Stream.close channel exn | None -> ()) channels;
  Eio.traceln ~__POS__ "Shutting down connection. Done";

  ()

(** Command handler. User commands (and channel commands) are serialized though this. *)
let command_handler ~set_close_reason ~command_stream ~service ~send_stream ~channels ~max_channels ~frame_max =
  ignore service; (* We should use the service to handle incoming requests (e.g. close and block), and to send commands to the server *)
  let rec loop next_channel =
    let next_channel = match Stream.receive command_stream with
      | Register_channel { receive_stream; set_blocked; promise } ->
        handle_register_channel
          ~channels ~send_stream ~max_channels ~frame_max ~next_channel ~set_blocked ~receive_stream promise
      | Deregister_channel { channel_no; promise } ->
        handle_deregister_channel channels channel_no promise;
        next_channel
      | Close reason ->
        (* Send close *)
        let exn = set_close_reason closed_by_user in
        let () = Spec.Connection.Close.client_request service ~reply_code:0 ~reply_text:reason ~class_id:0 ~method_id:0 () in
        raise exn
    in
    loop next_channel
  in
  loop 1

let send_command t command =
  Stream.send t.command_stream command

(**** User callable functions *****)

(** Register consumption for a free channel *)
let register_channel t ~set_blocked ~receive_stream =
  let pt, pu = Promise.create ~label:"Register channel" () in
  let command = Register_channel { receive_stream; set_blocked; promise = pu } in
  send_command t command;
  Promise.await pt

(** Deregister consumption for a given channel *)
let deregister_channel t ~channel_no =
  let pt, pu = Promise.create ~label:"Register channel" () in
  let command = Deregister_channel { channel_no; promise = pu } in
  send_command t command;
  Promise.await pt

let close t reason =
  let command = Close reason in
  send_command t command


let handle_start_message ~id ~credentials Spec.Connection.Start.{ version_major; version_minor; server_properties; mechanisms=_ ; locales } =
(*
   let print_map map =
   List.iter ~f:(fun (k, v) -> printf " %s = " k; Types.print_type " " v; printf "\n";) map
  in
  printf "Version: %d.%d\n" version_major version_minor;
  printf "Server properties:\n";
  print_map server_properties;
  printf "Mechanisms: %s\n" mechanisms;
  printf "Locales: %s\n" locales;
*)
  let server_product =
    List.assoc_opt "product" server_properties
    |> fun v -> Option.bind v (function Types.VLongstr s -> Some s | _ -> None)
    |> Option.value ~default:"<unknown>"
  in
  let server_version =
    List.assoc_opt "version" server_properties
    |> fun v -> Option.bind v (function Types.VLongstr s -> Some s | _ -> None)
    |> Option.value ~default:"<unknown>"
  in
  Eio.traceln ~__POS__ "Established connection to %s %s using AMQP %d.%d" server_product server_version version_major version_minor;

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
  in
  try
    loop ()
  with exn ->
    Eio.traceln ~__POS__ "Handle connection messages terminated with exn: %s" (Printexc.to_string exn);
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
  | End_of_file
  | Eio.Net.Connection_reset _ ->
    let exn = set_close_reason (Closed "Connection lost") in
    Eio.traceln ~__POS__ "Connection lost: %s" (Printexc.to_string exn);
    Stream.close (Option.get channels.(0) |> fst) exn
  | Closed _ as exn ->
    Eio.Flow.shutdown flow `Receive;
    raise exn
  | exn ->
    Eio.traceln ~__POS__ "Received exception: %s" (Printexc.to_string exn);
    raise exn


let send_messages ~set_close_reason flow send_stream =
  Framing.write_protocol_header flow;

  let rec loop () =
    (* let frames = receive send_stream in *)
    let frames = Stream.receive_all send_stream in
    Framing.write_frames flow frames;
    loop ()
  in
  try
    loop ()
  with
  | End_of_file
  | Eio.Net.Connection_reset _ ->
    let exn = set_close_reason (Closed "Connection lost") in
    Eio.traceln ~__POS__ "Connection closed: %s" (Printexc.to_string exn);
    Stream.close send_stream exn
  | Closed _ as exn ->
    Eio.traceln ~__POS__ "Send stream closed with exn: %s" (Printexc.to_string exn);
    Eio.Flow.shutdown flow `Send
  | exn ->
    Eio.traceln ~__POS__ "Recevied exception: %s" (Printexc.to_string exn);
    raise exn

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
  | exn -> Eio.traceln ~__POS__ "Heartbeat thread terminated with exn: %s" (Printexc.to_string exn);
    ()

let handle_blocked channels Spec.Connection.Blocked.{ reason } =
  Eio.traceln ~__POS__ "Connection blocked: %s" reason;
  Array.iter ~f:(function Some (_, set_blocked) -> set_blocked true | None -> ()) channels

let handle_unblocked channels () =
  Eio.traceln ~__POS__ "Connection unblocked";
  Array.iter ~f:(function Some (_, set_blocked) -> set_blocked false | None -> ()) channels

let handle_close ~send_stream ~command_stream set_close_reason Spec.Connection.Close.{ reply_code; reply_text; class_id; method_id } =
  let reason = Printf.sprintf "Connection closed by server. %d: %s. (%d, %d)" reply_code reply_text class_id method_id in
  let exn = set_close_reason (Closed reason) in
  (* Send reply and close the send stream *)
  let close_ok = Framing.create_method_frame Spec.Connection.Close_ok.def ~channel_no:0 () in
  Stream.close send_stream ~message:close_ok exn;
  Stream.close command_stream exn;
  Eio.traceln ~__POS__ "%s" (Printexc.to_string exn)

let set_close_reason, get_close_reason =
  let close_reason = ref None in
  (fun exn -> match !close_reason with Some exn -> exn | None -> (close_reason := (Some exn); exn)),
  (fun () -> !close_reason)

(** Create a channel to amqp *)
let init ~sw ~env ~id ?(virtual_host="/") ?heartbeat ?(max_frame_size=max_frame_size) ?(max_stream_length=50) ?(credentials=Credentials.default) ?(port=5672) host =
  let command_stream = Stream.create ~capacity:1 () in

  (* Create the streams *)
  let send_stream = Stream.create ~capacity:max_stream_length () in

  (* We just create max_channels. If the peer allows less, we waste a little memory *)
  let channels = Array.make max_channels None in


  Eio.Fiber.fork_sub
    ~sw
    ~on_error:(fun exn -> shutdown ~send_stream ~channels ~command_stream exn)
    (fun sw ->
       let clock = Eio.Stdenv.clock env in

       let addr = Eio_unix.Ipaddr.of_unix (Unix.inet_addr_of_string host) in
       let net = Eio.Stdenv.net env in
       let flow = Eio.Net.connect ~sw net (`Tcp (addr, port)) in

       (* Register channel 0 to handle messages *)
       let channel0_stream = Stream.create () in
       channels.(0) <- Some (channel0_stream, ignore);

       Eio.Fiber.fork ~sw (fun () -> send_messages ~set_close_reason flow send_stream);
       Eio.Fiber.fork ~sw (fun () -> receive_messages ~set_close_reason flow channels);

       let service = Service.init ~send_stream ~receive_stream:channel0_stream ~channel_no:0 () in
       Spec.Connection.Blocked.server_request service (handle_blocked channels);
       Spec.Connection.Unblocked.server_request service (handle_unblocked channels);

       (* We want to close the send stream after sending the message, so we cannot use the usual oneshot version *)
       Service.server_request Spec.Connection.Close.def service (handle_close ~command_stream ~send_stream set_close_reason);

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
             command_handler ~set_close_reason ~command_stream ~send_stream ~service ~channels ~max_channels:channel_max ~frame_max
           with exn -> Eio.traceln "Command handler closed: %s: %s" (Printexc.to_string exn) (Printexc.get_backtrace ()); raise exn);

       ()
    );
  { command_stream }
