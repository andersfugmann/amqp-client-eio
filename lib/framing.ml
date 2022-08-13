(** Framing helper functions *)
open StdLabels
open Utils

let protocol_header = "AMQP\x00\x00\x09\x01"

module Frame_header = struct
  type t = { frame_type: Types.Frame_type.t; channel: int; size: int }
  let spec = Protocol.Spec.(Octet :: Short :: Long :: [])
  let make frame_type channel size = { frame_type = Types.Frame_type.of_int frame_type; channel; size }
  let size = Protocol.Spec.size spec 0 0 0
  let read = Protocol.Spec.read spec make
  let write =
    let writer = Protocol.Spec.write spec in
    fun data off ~frame_type ~channel_no ~size -> writer data off (Types.Frame_type.to_int frame_type) channel_no size

  let buffer = Cstruct.create_unsafe size
end

module Frame_end = struct
  type t = unit
  let spec = Protocol.Spec.(Octet :: [])
  let make n = assert (n = Constants.frame_end)
  let size = Protocol.Spec.size spec 0
  let read = Protocol.Spec.read spec make
  let write =
    let writer = Protocol.Spec.write spec in
    fun data off () -> writer data off Constants.frame_end
end

module Method_header = struct
  type t = Types.message_id
  let spec = Protocol.Spec.(Short :: Short :: [])
  let make class_id method_id = Types.{ class_id; method_id }
  let apply f Types.{ class_id; method_id } = f class_id method_id
  let size = Protocol.Spec.size spec 0 0
  let read = Protocol.Spec.read spec make
  let write =
    let writer = Protocol.Spec.write spec in
    fun data off ~class_id ~method_id -> writer data off class_id method_id
end

(* Including flags. The spec allows for more than 15 content fields by adding an extra short if bit 0 is set.
   This is not supported! *)
module Content_header = struct
  type t = { class_id: int; weight: int; body_size: int; property_flags: int; }
  let spec = Protocol.Spec.(Short :: Short :: Longlong :: Short :: [])
  let make class_id weight body_size property_flags = { class_id; weight; body_size; property_flags }
  let size = Protocol.Spec.size spec 0 0 0 0
  let read = Protocol.Spec.(read spec make)
  let write =
    let writer = Protocol.Spec.write spec in
    fun data off ~class_id ~weight ~body_size ~property_flags -> writer data off class_id weight body_size property_flags
end

(* Should be completely moved to the service part *)
(* 'a is denoting the ???? *)
type content = { header: Content_header.t; data: Cstruct.t; body: Cstruct.t list }
type 'a handler = Types.Message_id.t -> Cstruct.t -> 'a expect_content
and 'a expect = { message_ids: Types.Message_id.t list; handler: 'a handler }
and 'a expect_content = Content of { class_id: int; handler: content -> 'a }
                      | Done of 'a

let read_data source buffer =
  Eio.Flow.read_exact source buffer;
  let hex_buf = Buffer.create 100 in
  Cstruct.hexdump_to_buffer hex_buf buffer;
  (* Eio.traceln "Read: (%d): %s" (Cstruct.length buffer) (Buffer.contents hex_buf); *)
  ()

let write_data flow data =
  let hex_buf = Buffer.create 100 in
  Cstruct.hexdump_to_buffer hex_buf data;
  (* Eio.traceln "Write: (%d): %s" (Cstruct.length data) (Buffer.contents hex_buf); *)
  let source = Eio.Flow.cstruct_source [data] in
  Eio.Flow.copy source flow;
  ()

let write_protocol_header flow =
  Printf.printf "Write: (%d)\n%!" (String.length protocol_header);
  String.iter ~f:(fun c -> Printf.printf "%02x " (Char.code c)) protocol_header;
  Printf.printf "\n%!";
  Eio.Flow.copy_string protocol_header flow

let read_frame source =
  read_data source Frame_header.buffer;
  (* Decode the header *)
  let frame_header = Frame_header.read Frame_header.buffer 0 in
  let data = Cstruct.create_unsafe (frame_header.Frame_header.size + Frame_end.size) in
  read_data source data;

  (* Assert that we see the frame end. *)
  Frame_end.read data frame_header.Frame_header.size;
  (frame_header, data)

let decode_method_header data =
  let message_header = Method_header.read data 0 in
  let body = Cstruct.sub data Method_header.size (Cstruct.length data - Method_header.size) in
  message_header, body

let decode_content_header data =
  let content_header = Content_header.read data 0 in
  let body = Cstruct.sub data Content_header.size (Cstruct.length data - Content_header.size) in
  content_header, body

let create_method_frame Protocol.Spec.{ message_id; spec; apply; _ } =
  (* Calculate the size of the complete frame *)
  let sizer = Protocol.Spec.size spec in
  let writer = Protocol.Spec.write spec in
  fun ~channel_no t ->
    let payload_size = apply sizer t in
    let data = Cstruct.create_unsafe (Frame_header.size + Method_header.size + payload_size + Frame_end.size) in
    let offset = Frame_header.write data 0 ~frame_type:Types.Frame_type.Method ~channel_no ~size:(Method_header.size + payload_size) in
    let offset = Method_header.write data offset ~class_id:message_id.class_id ~method_id:message_id.method_id in
    let offset = apply (writer data offset) t in
    let (_: int) = Frame_end.write data offset () in
    data

let create_method_frame_args def =
  let create_method_frame = create_method_frame def in
  fun ~channel_no ->
    def.make_named (create_method_frame ~channel_no)

let create_content_frame: _ Protocol.Content.def -> _ = fun def ->
  let sizer = Protocol.Content.size def.spec in
  let writer = Protocol.Content.write def.spec 0 in
  fun ~channel_no ~weight t ->
    let body_size = def.apply sizer t in
    let data = Cstruct.create_unsafe (Frame_header.size + Content_header.size + body_size + Frame_end.size) in
    let content_header_offset = Frame_header.write data 0 ~frame_type:Types.Frame_type.Method ~channel_no ~size:(Content_header.size + body_size) in
    (* Skip writing the content header, as we need to calculcate the flags first *)
    let offset = content_header_offset + Content_header.size in
    let property_flags = def.apply (writer data offset) t in
    let (_: int) = Frame_end.write data (offset + body_size) () in
    (* Write the content header *)
    let (_: int) = Content_header.write data content_header_offset ~class_id:def.message_id.class_id ~weight ~body_size ~property_flags in
    data


let heartbeat_frame =
  let size = Frame_header.size + 1 in
  let message = Cstruct.create size in
  let (_: int) = Frame_header.write message 0 ~frame_type:Types.Frame_type.Heartbeat ~channel_no:0 ~size:0 in
  let (_: int) = Frame_end.write message (Frame_header.size) () in
  message

let read_content receive_stream =
  let rec read_body = function
    | 0 -> []
    | n ->
      let (message_type, data) = Stream.receive receive_stream in
      assert (Types.Frame_type.equal message_type Types.Frame_type.Content_body);
      data :: read_body (n - Cstruct.length data)
  in
  let (message_type, data) = Stream.receive receive_stream in
  assert (Types.Frame_type.equal message_type Types.Frame_type.Content_header);
  (* Decode the content *)
  let header, data = decode_content_header data in
  let body = read_body header.body_size in
  { header; data; body }

let decode_content: ('t, _, _, _, _, _, _, _, _, _) Protocol.Content.def -> content -> ('t * Cstruct.t list) = fun def ->
  let decode = Protocol.Content.read def.spec in
  fun { header = { property_flags; _ }; data; body} ->
    decode def.make property_flags data 0, body

let decode_method: ('t, _, _, _, _, _, _, _, _, _) Protocol.Spec.def -> Cstruct.t -> 't = fun def ->
  let decode = Protocol.Spec.read def.spec in
  fun data -> decode def.make data 0

let read_method: _ Protocol.Spec.def -> _ = fun def ->
  let decode_method = decode_method def in
  fun data ->
    let message_id, data = decode_method_header data in
    assert (Types.Message_id.equal message_id def.message_id);
    decode_method data

let server_request_response: _ Protocol.Spec.def * _ Protocol.Spec.def -> _ = fun (req, rep) ->
  let read = Protocol.Spec.read req.spec in
  let create_method_frame = create_method_frame rep in
  let create_response ~stream ~channel_no data f =
    let t = read req.make data 0 in
    let t' = f (rep.make_named (fun x -> x)) t () in
    let packet = create_method_frame ~channel_no t' in
    Stream.send stream packet
  in
  req.message_id, create_response


(*type  ('t1, 'a1, 'b1, 'c1, 'd1, 'e1, ''t2, 'a2, 'b2, 'c2, 'd2, 'e2) request_reply =
  ('t1, 'a1, 'b1, 'c1, 'd1, 'e1) Protocol.Spec.def * ('t2, 'a2, 'b2, 'c2, 'd2, 'e2) Protocol.Spec.def
*)
let client_request_reply: _ Protocol.Spec.def * _ Protocol.Spec.def -> channel_no:int -> _ =
  fun (req, rep) ->
  let read_reply = Protocol.Spec.read rep.spec in
  let create_request = create_method_frame req in
  fun ~channel_no f reply_handler ->
    let handle_reply _message_id data =
      let reply = read_reply rep.make data 0 in
      Done (reply_handler reply)
    in
    let expect = { message_ids = [ rep.message_id ]; handler = handle_reply } in
    let request t =
      let message = create_request ~channel_no t in
      f (message, expect)
    in
    req.make_named request

let direct_client_request_reply def =
  let request_reply = client_request_reply def in
  fun send_stream receive_stream ~channel_no ->
    let f (message, expect) =
      let (_expect_message_id, handler) = match expect with
        | { message_ids = [ message_id ]; handler } -> message_id, handler
        | _ -> failwith "Only handle single type method replies"
      in
      Stream.send send_stream message;
      let (frame_type, reply) = Stream.receive receive_stream in
      assert (Types.Frame_type.equal Types.Frame_type.Method frame_type);
      let (message_id, data) = decode_method_header reply in
      match handler message_id data with
      | Done t -> t
      | _ -> failwith "No more messages expected"
    in
    request_reply f (fun t -> t) ~channel_no


let server_request:
  ('request_t, 'input, 'output, 'make, 'make_named, 'make_named_result, 'apply, 'apply_result, 'apply_named, 'apply_named_result) Protocol.Spec.def ->
  channel_no:int -> 'apply_named -> unit expect =
  fun req ->
  let read_request = Protocol.Spec.read req.spec in
  fun ~channel_no:_ f ->
    let handler message_id data =
      assert (Types.Message_id.equal message_id req.message_id);
      let request = read_request req.make data 0 in
      let () = req.apply_named f request in
      Done ()
    in
    { message_ids = [req.message_id]; handler }


let server_request_reply:
  ('request_t, 'input1, 'output1, 'make1, 'make_named1, 'make_named_result1, 'apply1, 'apply_result1, 'apply_named1, 'apply_named_result1) Protocol.Spec.def *
  ('reply_t, 'input2, 'output2, 'make2, 'make_named2, 'make_named_result2, 'apply2, 'apply_result2, 'apply_named2, 'apply_named_result2) Protocol.Spec.def ->
  channel_no:int -> 'apply_named1 -> Cstruct.t expect =
  fun (req, rep) ->
  let read_request = Protocol.Spec.read req.spec in
  let create_reply = create_method_frame rep in
  fun ~channel_no f ->
    let handler message_id data =
      assert (Types.Message_id.equal message_id req.message_id);
      let request = read_request req.make data 0 in
      let reply = req.apply_named f request in
      let message = create_reply ~channel_no reply in
      Done message
    in
    { message_ids = [req.message_id]; handler }

let direct_server_request_reply:
  ('request_t, 'input1, 'output1, 'make1, 'make_named1, 'make_named_result1, 'apply1, 'apply_result1, 'apply_named1, 'apply_named_result1) Protocol.Spec.def *
  ('reply_t, 'input2, 'output2, 'make2, 'make_named2, 'make_named_result2, 'apply2, 'apply_result2, 'apply_named2, 'apply_named_result2) Protocol.Spec.def -> _ Stream.t -> _ Stream.t -> channel_no:int -> 'apply_named1 -> 'reply_t  = fun ((req, _rep) as def) ->

  let server_request_reply = server_request_reply def in
  fun receive_stream send_stream ~channel_no f ->
    let reply_t = ref None in
    let wrap_f : 'apply_named1 =
      let f': 'request_t -> 'response_t = fun t ->
        let reply = req.apply_named f t in
        reply_t := Some reply;
        reply
      in
      req.make_named f'
    in
    let _message_id, handler =
      match server_request_reply ~channel_no wrap_f with
      | { message_ids = [ message_id ]; handler } -> message_id, handler
      | _ -> failwith "Server requests can only be Methods"
    in
    let (frame_type, data) = Stream.receive receive_stream in
    assert (Types.Frame_type.equal Types.Frame_type.Method frame_type);
    let (message_id, data) = decode_method_header data in
    let () =
      match handler message_id data with
      | Done reply -> Stream.send send_stream reply
      | _ -> failwith "Result of a server request must a message reply"
    in
    Option.get !reply_t
