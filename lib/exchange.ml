open !StdLabels
open Spec.Exchange

(* type match_type = Any | All *)

type _ exchange_type =
  | Direct: (queue:string -> unit) exchange_type
  | Fanout: unit exchange_type
  | Topic:  (topic:string -> unit) exchange_type
  | Match:  (headers:Types.header list -> unit) exchange_type

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

let bind: type a. _ Channel.t -> destination: _ t -> source: a t -> a =
  fun channel ~destination ~source ->
  let bind ?(routing_key="") ?(arguments=[]) () =
    Bind.client_request channel.service ~destination:destination.name ~source:source.name ~routing_key ~no_wait:false ~arguments ()
  in
  match source.exchange_type with
  | Direct -> fun ~queue -> bind ~routing_key:queue ()
  | Fanout -> bind ()
  | Topic -> fun ~topic -> bind ~routing_key:topic ()
  | Match -> fun ~headers -> bind ~arguments:headers ()

let unbind: type a. _ Channel.t -> destination: _ t -> source: a t -> a =
  fun channel ~destination ~source ->
  let unbind ?(routing_key="") ?(arguments=[]) () =
    Unbind.client_request channel.service ~destination:destination.name ~source:source.name ~routing_key ~no_wait:false ~arguments ()
  in
  match source.exchange_type with
  | Direct -> fun ~queue -> unbind ~routing_key:queue ()
  | Fanout -> unbind ()
  | Topic -> fun ~topic -> unbind ~routing_key:topic ()
  | Match -> fun ~headers -> unbind ~arguments:headers ()

let create_publish = Framing.create_method_frame Spec.Basic.Publish.def
let create_content = Framing.create_content_frame Spec.Basic.Content.def

module Raw = struct
  let publish: type a. _ t -> a Channel.t -> ?mandatory:bool -> routing_key:string -> Spec.Basic.Content.t * Cstruct.t list -> a =
  fun t channel ?(mandatory=false) ~routing_key (content, body) ->
  let publish =
    create_publish ~channel_no:channel.channel_no
      Spec.Basic.Publish.{ exchange = t.name;
                           routing_key;
                           mandatory;
                           immediate = false;
                         }
  in
  let content = create_content ~channel_no:channel.channel_no ~body_size:(Cstruct.lenv body) content in
  let body = Framing.create_raw_body_frames ~max_frame_size:channel.frame_max ~channel_no:channel.channel_no body in
  let data = publish :: content :: body in
  Channel.publish channel data

end

let publish: type a. _ t -> a Channel.t -> ?mandatory:bool -> routing_key:string -> Message.content -> a =
  fun t channel ?(mandatory=false) ~routing_key (content, body) ->
  let publish =
    create_publish ~channel_no:channel.channel_no
      Spec.Basic.Publish.{ exchange = t.name;
                           routing_key;
                           mandatory;
                           immediate = false;
                         }
  in
  let content = create_content ~channel_no:channel.channel_no ~body_size:(String.length body) content in
  let body = Framing.create_body_frames ~max_frame_size:channel.frame_max ~channel_no:channel.channel_no body in
  let data = publish :: content :: body in
  Channel.publish channel data
