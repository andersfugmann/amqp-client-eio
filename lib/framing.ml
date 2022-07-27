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

let read_data source buffer =
  Eio.Flow.read_exact source buffer;
  Printf.printf "Read: (%d)%!" (Cstruct.length buffer);
  Cstruct.hexdump buffer;
  Printf.printf "\n%!";
  ()

let write_data flow data =
  Printf.printf "Write: (%d)%!" (Cstruct.length data);
  Cstruct.hexdump data;
  Printf.printf "\n%!";
  let source = Eio.Flow.cstruct_source [data] in
  Eio.Flow.copy source flow

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
    def.init (create_method_frame ~channel_no)


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

type content = { header: Content_header.t; data: Cstruct.t; body: Cstruct.t list }

let read_content =
  let Protocol.Content.{ message_id = { class_id; _}; _ } = Spec.Basic.Content.def in
  fun receive_stream ->
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
    assert (header.class_id = class_id);
    (* let content = read_content 0 content_data content_header.property_flags in *)
    let body = read_body header.body_size in
    { header; data; body }

let read_method: _ Protocol.Spec.def -> _ = fun def ->
  let read = Protocol.Spec.read def.spec in
  fun data ->
    let message_id, data = decode_method_header data in
    assert (Types.Message_id.equal message_id def.message_id);
    read def.make data 0

let server_request_response: _ Protocol.Spec.def * _ Protocol.Spec.def -> _ = fun (req, rep) ->
  let read = Protocol.Spec.read req.spec in
  let create_method_frame = create_method_frame rep in
  let create_response ~stream ~channel_no data f =
    let t = read req.make data 0 in
    let t' = f (rep.init (fun x -> x)) t in
    let packet = create_method_frame ~channel_no t' in
    Stream.send stream packet
  in
  req.message_id, create_response
