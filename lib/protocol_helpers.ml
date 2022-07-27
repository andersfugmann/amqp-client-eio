(* Move helper functions to the framing layer and implement actual calls in modules


(* For persistant message listeners, we should have a stream *)

type send_receive = Cstruct.t list -> ?persistant:bool -> Types.message_id list -> (Types.message_id * Cstruct.t) option

(** Send a request, but do not expect a reply. Content frames needs to be set!, and we need to call make *)
let send_request_0: _ spec_def -> send_receive -> channel_no:int -> _ = fun { message_id; spec; make = _; apply; init } ->
  let create_method_frame = Framing.create_method_frame spec apply in
  fun f ~channel_no ->
    let process_t t =
      let frame = create_method_frame ~channel_no ~message_id t in
      let (_: _ option) = f [frame] [] in
      ()
    in
    init process_t

(* Register for reception. *)
(* We register for reception of a message using message_id, and then receive a message.  *)
(* Then return t.  *)
let receive_request_0: _ spec_def -> send_receive -> _ = fun { message_id; spec; make; apply = _; init = _ } ->
  let read_message = Protocol.Spec.read spec make in
  fun f ~persistant ->
    match f [] [message_id] ~persistant with
    | None -> failwith "Did not receive any message"
    | Some (_message_id, message) -> read_message message 0


(** Send and then receive one message *)
let send_request_1: _ spec_def -> _ spec_def -> send_receive -> _ = fun { message_id = send_message_id; spec = send_spec; make = _; apply; init } { message_id = receive_message_id; spec = receive_spec; make; apply = _; init = _ } rep ->

  let create_message = Framing.create_method_frame send_spec apply in
  let read_message = Protocol.Spec.read receive_spec make in
  fun f ~channel_no ~persistant:false ->
    let process_t t =
      let frame = create_method_frame ~channel_no ~message_id t in
      let (_: _ option) = f [frame] [] in
      ()
    in
    init process_t



  let create_message = Framing.create_method_frame send_spec apply in

  let send_request = send_request_0 req in
  let receive_request = receive_request_0 rep in
  (* Nah.. We cannot reuse... Yet *)
  (* We need a send function and a receive function *)
  (* Register reception of the message. Oops. We cannot do that. Sending and registering must happen synchroniously *)


  fun send_func receive_func ~channel_no ->
    send_request ~channel_no send_func




let send_request_2: _ spec_def -> _ spec_def -> _ -> _ spec_def -> _ -> unit = fun _req _rep1_c _rep1 _rep2_c _rep2 -> ()

let receive_request_1: _ spec_def -> _ spec_def -> unit = fun _req _rep -> ()

(* Receiving is more difficult. *)
(* Get -> `Ok of content + data -> `Empty *)
(* Lets create these manually! *)
*)
