
(** AMQP client connection *)

open StdLabels
open Utils

exception Closed of string

(* We can place framing in a seperate module, but is should only contain helper functions. *)
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

(** Add block / unblock *)
type command = Register_channel of { channel_stream: channel_stream; promise: (int * send_stream) Promise.u }
             | Deregister_channel of { channel_no: int; promise: unit Promise.u }
             | Close
             | Block
             | Unblock

type t = {
  send_stream: send_stream;
  channels: channel_stream option array;
  command_stream: command Stream.t;
  frame_max: int;
  mutable next_channel: int;
  mutable blocked: (unit Promise.t * unit Promise.u);
  mutable close_reason: exn option;
}

let get_send_stream { send_stream; _ } = send_stream

let handle_register_channel ({ channels; send_stream; next_channel; _ } as t) channel_stream promise =
  let total_channels = Array.length channels in
  (* Find the next free channel no *)
  let rec find_next_free_channel idx = function
    | 0 -> failwith "No available channels"
    | n -> match channels.(idx) with
      | None -> idx
      | Some _ -> find_next_free_channel ((idx + 1) mod total_channels) (n - 1)
  in
  let channel_no = find_next_free_channel next_channel total_channels in
  (* Store back the indicator for next possible free channel number *)
  t.next_channel <- (channel_no mod total_channels);
  t.channels.(channel_no) <- Some channel_stream;
  Promise.resolve_ok promise (channel_no, send_stream)

let handle_deregister_channel {channels; _ } channel_no promise =
  channels.(channel_no) <- None;
  Promise.resolve_ok promise ()

let shutdown t reason =
  (* Should we close the socket now... Yes! *)
  Promise.resolve_exn (snd t.blocked) reason;
  Stream.close t.send_stream reason;
  Array.iter ~f:(function Some channel -> Stream.close channel reason | None -> ()) t.channels;
  ()

(* Will be terminated when / if any other fiber is closed *)
let rec command_handler t =
  let command =
    try
      Stream.receive t.command_stream
    with reason ->
      shutdown t reason;
      raise reason;
  in
  let () =
    match command with
    | Register_channel { channel_stream; promise } ->
      handle_register_channel t channel_stream promise
    | Deregister_channel { channel_no; promise } ->
      handle_deregister_channel t channel_no promise
    | _ -> failwith "unsupported command"
  in
  command_handler t

let send_command t command =
  Stream.send t.command_stream command

(**** User callable functions *****)

(** Register consumption for a free channel *)
let register_channel t channel_stream =
  let pt, pu = Promise.create ~label:"Register channel" () in
  let command = Register_channel { channel_stream; promise = pu } in
  send_command t command;
  Promise.await pt

(** Deregister consumption for a given channel *)
let deregister_channel t ~channel_no =
  let pt, pu = Promise.create ~label:"Register channel" () in
  let command = Deregister_channel { channel_no; promise = pu } in
  send_command t command;
  Promise.await pt

(* This function is purposefully not lifting partially applied functions, as its only called once and is not performance critical *)
let direct_read: _ Protocol.Spec.def -> _ -> _ = fun Protocol.Spec.{ message_id; spec; make; _ } source ->
  let reader = Protocol.Spec.read spec in
  let (Framing.Frame_header.{ frame_type; channel; _ }, data) = Framing.read_frame source in
  assert (Types.Frame_type.equal Types.Frame_type.Method frame_type);
  assert (channel = 0);

  let message_id', data = Framing.decode_method_header data in
  assert (Types.Message_id.equal message_id message_id');
  reader make data 0

(* This function is purposefully not lifting partially applied functions, as its only called once and is not performance critical *)
let direct_write: _ Protocol.Spec.def -> _ -> _ = fun def sink ->
  let open Framing in
  let send t =
    let message = create_method_frame def ~channel_no:0  t in
    Framing.write_data sink message
  in
  def.init send

let reply_start flow ~id ~credentials =
  let print_map map =
    List.iter ~f:(fun (k, v) -> printf " %s = " k; Types.print_type " " v; printf "\n";) map
  in
  let (start_def, start_ok_def) = Spec.Connection.Start.reply in

  let Spec.Connection.Start.{
    version_major;
    version_minor;
    server_properties;
    mechanisms;
    locales; } =
    direct_read start_def flow
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
  direct_write start_ok_def flow ~client_properties ~mechanism ~response ~locale ()

let reply_tune ?heartbeat flow =
  let (tune_def, tune_ok_def) = Spec.Connection.Tune.reply in

  let tune = direct_read tune_def flow in
  let channel_max = Int.max max_channels tune.channel_max in
  let frame_max = Int.max max_frame_size tune.frame_max in
  let heartbeat = match heartbeat with
    | None -> tune.heartbeat
    | Some n -> n
  in

  (* Produce a reply *)
  direct_write tune_ok_def flow ~channel_max ~frame_max ~heartbeat ();
  (channel_max, frame_max, heartbeat)

let request_open flow ~virtual_host =
  let (open_def, open_ok_def) = Spec.Connection.Open.request in
  direct_write open_def flow ~virtual_host ();
  let t = direct_read open_ok_def flow in
  t

(** Handle connection messages *)
let handle_connection_message t frame_type data =
  let close_id, close_f = Framing.server_request_response Spec.Connection.Close.reply in
  match frame_type with
  | Types.Frame_type.Heartbeat ->
    (* Happy times. We received a heartbeat *)
    ()
  | Content_header
  | Content_body ->
    failwith "Channel 0 should never receive content messages"
  | Method ->
    begin
      let message_id, data = Framing.decode_method_header data in
      match message_id with
      | message_id when Types.Message_id.equal message_id Spec.Connection.Blocked.message_id ->
        ()
      | message_id when Types.Message_id.equal message_id Spec.Connection.Unblocked.message_id ->
        ()
      | message_id when Types.Message_id.equal message_id close_id ->
        let close_connection t message =
          let reason =
            match message with
            | Spec.Connection.Close.{ reply_code; reply_text; class_id = 0; method_id = 0 } ->
              Printf.sprintf "%d: %s" reply_code reply_text
            | { reply_code; reply_text; class_id; method_id } ->
              Printf.sprintf "%d: Server terminated connection due to protocol failure. Text: %s. Offending message id: (%d, %d)" reply_code reply_text class_id method_id;
          in
          Stream.close t.command_stream (Closed reason);
        in
        close_f ~stream:t.send_stream ~channel_no:0 data (fun init close -> close_connection t close; init ());
      | _ -> failwith "Unknown message on channel 0"
    end

let rec receive_messages t flow =
  let (frame_header, data) =
    try
      Framing.read_frame flow
    with _ ->
      let exn = Closed "Network connection terminated" in
      Stream.close t.command_stream exn;
      raise exn
  in
  let () =
    match frame_header.channel with
    | 0 ->
      (* I think a channel would be more suited here! - but we can post message to a function if needed *)
      handle_connection_message t frame_header.frame_type data
    | n ->
      match t.channels.(n) with
      | None -> (* Do some error handling of the message - we dont know it *)
        Log.error "Data received on closed channel: %d" n;
      | Some channel ->
        (* The connection stream should never close *)
        try
          Stream.send channel (frame_header.frame_type, data)
        with _ ->
          failwith "Channels should not be closed before removing!"
  in
  receive_messages t flow

let rec send_messages t flow =
  let frame =
    try
      Stream.receive t.send_stream
    with
    | exn ->
      (* Someone closed the stream. Just exit *)
      Eio.Flow.shutdown flow `Send;
      raise exn (* We don't really need to raise tbh *)
  in
  let () =
    try
      Framing.write_data flow frame
    with _ ->
      let exn = Closed "Network connection terminated" in
      Stream.close ~notify_consumers:false ~flush:true t.send_stream exn;
      Stream.close ~notify_consumers:true ~flush:false t.command_stream exn;
      raise exn
  in
  send_messages t flow

(** Continuously send a heartbeat frame every [freq] / 2 *)
let rec send_heartbeat ~clock stream freq =
  (* Should verify that there has been a message from the server withing [freq] seconds, or terminate the connection if not. *)
  let freq' = Float.of_int freq /. 2.0 in

  Eio.Time.sleep clock freq';
  (* Only send if the stream is empty. We could optimize this to
     record last send, and only send when idle.
  *)
  if Stream.is_empty stream; then
    Stream.send stream Framing.heartbeat_frame;

  send_heartbeat ~clock stream freq


(** Create a channel to amqp *)
let init ~sw ~env ~id ?(virtual_host="/") ?heartbeat ?(max_stream_length=5) ?(credentials=Credentials.default) ?(port=5672) host =

  let addr = Eio_unix.Ipaddr.of_unix (Unix.inet_addr_of_string host) in
  let net : #Eio.Net.t = Eio.Stdenv.net env in
  let flow = Eio.Net.connect ~sw net (`Tcp (addr, port)) in

  (* Move flow somewhere else *)
  (* Use the send and receive handler *)

  (* Move to the send stream. Send protocol header *)
  Framing.write_protocol_header flow;

  let () = reply_start flow ~id ~credentials in
  let (channel_max, frame_max, heartbeat_freq) = reply_tune ?heartbeat flow in

  (* Start thread to send heartbeats. *)
  let send_stream = Stream.create ~max_size:max_stream_length () in
  let () = request_open flow ~virtual_host in

  let t = { send_stream;
            channels = Array.make channel_max None;
            command_stream = Stream.create ~max_size:1 ();
            frame_max;
            next_channel = 1;
            blocked = Promise.create ();
            close_reason = None;
          }
  in

  Eio.Fiber.fork_sub
    ~sw
    ~on_error:(fun exn ->
      Printf.printf "Connection closed: %s\n%!" (Printexc.to_string exn);
      match t.close_reason with None -> t.close_reason <- Some exn | Some _ -> ())
    (fun sw ->
       let clock = Eio.Stdenv.clock env in
       Eio.Fiber.fork ~sw (fun () -> try send_messages t flow with exn -> Printf.printf "Send message closed: %s\n%!" (Printexc.to_string exn));
       Eio.Fiber.fork ~sw (fun () -> try receive_messages t flow with exn -> Printf.printf "Receive message closed: %s\n%!" (Printexc.to_string exn));

       Eio.Fiber.fork ~sw (fun () -> try send_heartbeat ~clock send_stream heartbeat_freq with exn -> Printf.printf "Heartbeat closed: %s\n%!" (Printexc.to_string exn));

       Eio.Fiber.fork ~sw (fun () -> try command_handler t with exn -> Printf.printf "Command handler closed: %s\n%!" (Printexc.to_string exn); raise exn);

    );
  t
