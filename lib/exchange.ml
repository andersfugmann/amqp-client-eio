open !StdLabels
open Spec.Exchange

(* type match_type = Any | All *)

type _ exchange_type =
  | Direct: [`Queue of string] exchange_type
  | Fanout: unit exchange_type
  | Topic:  [`Topic of string] exchange_type
  | Match:  [`Headers of Types.header list] exchange_type

let direct_t = Direct
let fanout_t = Fanout
let topic_t = Topic
let match_t = Match

type 'a t = { name : string;
              exchange_type: 'a exchange_type }

(** Predefined Default exchange *)
let default    = { name=""; exchange_type = Direct }

(** Predefined Direct exchange *)
let amq_direct = { name = "amq.direct"; exchange_type = Direct }

(** Predefined Fanout exchange *)
let amq_fanout = { name = "amq.fanout";  exchange_type = Fanout }

(** Predefined topic exchange *)
let amq_topic  = { name = "amq.topic"; exchange_type = Topic }

(** Predefined match (header) exchange *)
let amq_match = { name = "amq.match"; exchange_type = Match }

let string_of_exchange_type: type a. a exchange_type -> string  = function
  | Direct -> "direct"
  | Fanout -> "fanout"
  | Topic -> "topic"
  | Match -> "match"

let declare: type a. ?passive:bool -> ?durable:bool -> ?auto_delete:bool -> ?internal:bool ->
  _ Channel.t -> a exchange_type -> ?arguments:Types.table -> string -> a t =
  fun ?(passive=false) ?(durable=false) ?(auto_delete=false) ?(internal=false)
    channel exchange_type ?(arguments=[]) name ->

    let () = Declare.client_request channel.service
      ~exchange:name ~amqp_type:(string_of_exchange_type exchange_type)
      ~passive ~durable ~auto_delete ~internal ~no_wait:false ~arguments ()
    in
    { name; exchange_type }

let delete: _ Channel.t -> ?if_unused:bool -> _ t -> unit = fun channel ?(if_unused=false) { name; _ } ->
  Delete.client_request channel.service ~exchange:name ~if_unused ~no_wait:false ()

let bind: type a. _ Channel.t -> destination: _ t -> source: a t -> a -> unit =
  fun channel ~destination ~source ->
  let bind ?(routing_key="") ?(arguments=[]) () =
    Bind.client_request channel.service ~destination:destination.name ~source:source.name ~routing_key ~no_wait:false ~arguments ()
  in
  match source.exchange_type with
  | Direct -> fun (`Queue routing_key) -> bind ~routing_key ()
  | Fanout -> fun () -> bind ()
  | Topic -> fun (`Topic routing_key) -> bind ~routing_key ()
  | Match -> fun (`Headers arguments) -> bind ~arguments ()

let unbind: type a. _ Channel.t -> destination: _ t -> source: a t -> a -> unit =
  fun channel ~destination ~source ->
  let unbind ?(routing_key="") ?(arguments=[]) () =
    Unbind.client_request channel.service ~destination:destination.name ~source:source.name ~routing_key ~no_wait:false ~arguments ()
  in
  match source.exchange_type with
  | Direct -> fun (`Queue routing_key) -> unbind ~routing_key ()
  | Fanout -> fun () -> unbind ()
  | Topic -> fun (`Topic routing_key) -> unbind ~routing_key ()
  | Match -> fun (`Headers arguments) -> unbind ~arguments ()

let create_publish = Framing.create_method_frame Spec.Basic.Publish.def
let create_content = Framing.create_content_frames Spec.Basic.Content.def

let publish: type a. _ t -> a Channel.t -> ?mandatory:bool -> routing_key:string -> Message.content -> a =
  fun t channel ?(mandatory=false) ~routing_key (content, body) ->
  Eio.traceln "Publish called";
  let publish =
    create_publish ~channel_no:channel.channel_no
      Spec.Basic.Publish.{ exchange = t.name;
                           routing_key;
                           mandatory;
                           immediate = false;
                         }
  in
  let content = create_content ~max_frame_size:channel.frame_max ~channel_no:channel.channel_no content body in
  let data = publish :: content in
  Channel.publish channel data
