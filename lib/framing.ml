(** Framing helper functions *)
open StdLabels
open Utils

let trace = false

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
    fun data off ->
      let (_ : int) = writer data off Constants.frame_end in
      ()
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
    fun data off ~class_id ~body_size ~property_flags -> writer data off class_id 0 body_size property_flags
end

type content = { header: Content_header.t; data: Cstruct.t; body: Cstruct.t list }

let read_data source buffer =
  Eio.Flow.read_exact source buffer;
  let hex_buf = Buffer.create 100 in
  Cstruct.hexdump_to_buffer hex_buf buffer;
  if trace then Eio.traceln "Read: (%d): %s" (Cstruct.length buffer) (Buffer.contents hex_buf);
  ()

let write_frames flow frames =
  if (trace) then begin
    List.iter ~f:(fun data ->
      let hex_buf = Buffer.create 100 in
      Cstruct.hexdump_to_buffer hex_buf data;
      Eio.traceln "Write: (%d): %s" (Cstruct.length data) (Buffer.contents hex_buf);
    ) frames
  end;
  let source = Eio.Flow.cstruct_source frames in
  Eio.Flow.copy source flow;
  ()

let write_protocol_header flow =
  if (trace) then begin
    Eio.traceln "Write: (%d)" (String.length protocol_header);
    String.fold_right protocol_header ~f:(fun c acc -> Printf.sprintf "%02x" (Char.code c) :: acc) ~init:[]
    |> String.concat ~sep:" "
    |> Eio.traceln "%s"
  end;
  Eio.Flow.copy_string protocol_header flow

(* When receiving the body, we could hold partially received data. This allows us to do zero copy.
   However, it means that we need obtain the size of the body content by decoding the content header *)
let read_frame source =
  read_data source Frame_header.buffer;
  (* Decode the header *)
  let frame_header = Frame_header.read Frame_header.buffer 0 in
  let data = Cstruct.create_unsafe (frame_header.Frame_header.size + Frame_end.size) in
  read_data source data;

  (* Assert that we see the frame end. *)
  Frame_end.read data frame_header.Frame_header.size;
  (frame_header, Cstruct.sub data 0 frame_header.Frame_header.size)

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
    Frame_end.write data offset;
    data

let create_method_frame_args def =
  let create_method_frame = create_method_frame def in
  fun ~channel_no ->
    def.make_named (create_method_frame ~channel_no)

let create_raw_body_frames acc ~channel_no data =
  let frame = Cstruct.create_unsafe (Frame_header.size + Frame_end.size) in
  let frame_offset = Frame_header.write frame 0 ~frame_type:Types.Frame_type.Content_body ~channel_no ~size:(Cstruct.length data) in
  Frame_end.write frame frame_offset;
  let frame_start = Cstruct.sub frame 0 Frame_header.size in
  let frame_end = Cstruct.sub frame frame_offset Frame_end.size in
  frame_end :: data :: frame_start :: acc

let create_raw_body_frames ~max_frame_size ~channel_no segments =
  let rec loop acc = function
    | [] -> acc
    | x :: xs when Cstruct.length x = 0 -> loop acc xs
    | x :: xs when Cstruct.length x > max_frame_size ->
      let data = Cstruct.sub x 0 max_frame_size in
      let data' = Cstruct.sub x max_frame_size (Cstruct.length x - max_frame_size) in
      loop (create_raw_body_frames acc ~channel_no data) (data' :: xs)
    | x :: xs ->
      loop (create_raw_body_frames acc ~channel_no x) xs
  in
  segments |> List.rev |> loop [] |> List.rev

let create_content_frame: _ Protocol.Content.def -> channel_no:int -> body_size:int -> 'content -> Cstruct.t = fun def ->
  let sizer = Protocol.Content.size def.spec in
  let writer = Protocol.Content.write def.spec 0 in

  fun ~channel_no ~body_size t  ->
    let content_size = def.apply sizer t in
    let content = Cstruct.create_unsafe (Frame_header.size + Content_header.size + content_size + Frame_end.size) in
    let content_header_offset = Frame_header.write content 0 ~frame_type:Types.Frame_type.Content_header ~channel_no ~size:(Content_header.size + content_size) in

    let property_flags = def.apply (writer content (content_header_offset + Content_header.size)) t in
    let (_: int) = Content_header.write content content_header_offset ~class_id:def.message_id.class_id ~body_size ~property_flags in
    Frame_end.write content (Cstruct.length content - 1);
    content

let create_body_frame ~channel_no ~offset ~length body =
  let frame = Cstruct.create_unsafe (Frame_header.size + length + Frame_end.size) in
  let frame_offset = Frame_header.write frame 0 ~frame_type:Types.Frame_type.Content_body ~channel_no ~size:length in
  Cstruct.blit_from_string body offset frame frame_offset length;
  Frame_end.write frame (frame_offset + length);
  frame

(* Create at least one body frame, even if the content is 0 length. Dont really know if we want that.... *)
let create_body_frames ~max_frame_size ~channel_no body =
  let length = String.length body in
  let rec loop offset =
    match length - offset with
    | 0 -> []
    | n ->
      let length = Int.min max_frame_size n in
      create_body_frame ~offset ~channel_no ~length body :: loop (offset + length)
  in
  loop 0

let heartbeat_frame =
  let size = Frame_header.size + 1 in
  let message = Cstruct.create size in
  let (_: int) = Frame_header.write message 0 ~frame_type:Types.Frame_type.Heartbeat ~channel_no:0 ~size:0 in
  Frame_end.write message (Frame_header.size);
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

let decode_content: ('t, _, _, _, _, _, _, _) Protocol.Content.def -> content -> ('t * Cstruct.t list) = fun def ->
  let decode = Protocol.Content.read def.spec in
  fun { header = { property_flags; _ }; data; body} ->
    decode def.make property_flags data 0, body

let decode_method: ('t, _, _, _, _, _, _, _) Protocol.Spec.def -> Cstruct.t -> 't = fun def ->
  let decode = Protocol.Spec.read def.spec in
  fun data -> decode def.make data 0

let read_method: _ Protocol.Spec.def -> _ = fun def ->
  let decode_method = decode_method def in
  fun data ->
    let message_id, data = decode_method_header data in
    assert (Types.Message_id.equal message_id def.message_id);
    decode_method data
