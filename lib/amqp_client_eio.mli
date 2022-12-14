module Types : sig
  type bit = bool
  and octet = int
  and short = int
  and long = int
  and longlong = int
  and shortstr = string
  and longstr = string
  and timestamp = int
  and decimal = Types.decimal = { digits : int; value : int; }
  and table = (string * value) list
  and array = value list
  and value =
    Types.value =
      VBoolean of bool
    | VShortshort of int
    | VShort of int
    | VLong of int
    | VLonglong of int
    | VShortstr of string
    | VLongstr of string
    | VFloat of float
    | VDouble of float
    | VDecimal of decimal
    | VTable of table
    | VArray of value list
    | VTimestamp of int
    | VUnit of unit
  type header = longstr * value
end

module Connection : sig

  module Credentials: sig
    type t = {
      username : string;
      password : string;
      mechanism : string;
    }
    val make : username:string -> password:string -> t
    val default : t
  end
  exception Closed of string
  type t
  val init :
    sw:Eio.Switch.t ->
    env:Eio.Stdenv.t ->
    id:string ->
    ?virtual_host:string ->
    ?heartbeat:int ref ->
    ?max_frame_size:int ->
    ?max_stream_length:int ->
    ?credentials:Credentials.t -> ?port:int -> string -> t
  val close: t -> string -> unit
end

module Channel : sig
  type 'a t
  type 'a confirm
  val no_confirm : unit confirm
  val with_confirm : [ `Ok | `Rejected ] confirm
  exception Closed of string
  exception Channel_closed of Spec.Channel.Close.t
  val init : sw:Eio.Switch.t -> Connection.t -> 'a confirm -> 'a t
end

module Message : sig
  type content = Spec.Basic.Content.t * string
  type content_raw = Spec.Basic.Content.t * Cstruct.t list

  type deliver = Spec.Basic.Deliver.t =
    { consumer_tag : string;
      delivery_tag : int;
      redelivered : bool;
      exchange : string;
      routing_key : string;
    }

  type t = deliver * content
  type t_raw = deliver * content_raw

  val make :
    ?content_type:string ->
    ?content_encoding:string ->
    ?headers:Types.table ->
    ?delivery_mode:int ->
    ?priority:int ->
    ?correlation_id:string ->
    ?reply_to:string ->
    ?expiration:int ->
    ?message_id:string ->
    ?timestamp:int ->
    ?amqp_type:string ->
    ?user_id:string -> ?app_id:string -> string -> content
  val ack : 'a Channel.t -> ?multiple:bool -> deliver -> unit
  val reject : 'a Channel.t -> ?multiple:bool -> requeue:bool -> deliver -> unit
  val recover : 'a Channel.t -> requeue:bool -> unit
end

module Exchange : sig
  type 'a t
  type 'a exchange_type

  val direct_t : (queue:string -> unit) exchange_type
  val fanout_t : unit exchange_type
  val topic_t : (topic:string -> unit) exchange_type
  val match_t : (headers:Types.header list -> unit) exchange_type
  val default : (queue:string -> unit) t
  val amq_direct : (queue:string -> unit) t
  val amq_fanout : unit t
  val amq_topic : (topic:string -> unit) t
  val amq_match : (headers:Types.header list -> unit) t

  val declare :
    ?passive:bool ->
    ?durable:bool ->
    ?auto_delete:bool ->
    ?internal:bool ->
    'b Channel.t ->
    'a exchange_type -> ?arguments:Types.table -> string -> 'a t
  val delete : 'a Channel.t -> ?if_unused:bool -> 'b t -> unit
  val bind : 'b Channel.t -> destination:'c t -> source:'a t -> 'a
  val unbind : 'b Channel.t -> destination:'c t -> source:'a t -> 'a
  val publish :
    _ t -> 'a Channel.t -> ?mandatory:bool -> routing_key:string -> Message.content -> 'a
  module Raw : sig
    val publish :
      _ t -> 'a Channel.t -> ?mandatory:bool -> routing_key:string -> Message.content_raw -> 'a
  end
end


module Queue : sig
  type t
  val message_ttl : int -> string * Types.value
  val auto_expire : int -> string * Types.value
  val max_length : int -> string * Types.value
  val max_length_bytes : int -> string * Types.value
  val dead_letter_exchange : string -> string * Types.value
  val dead_letter_routing_key : string -> string * Types.value
  val maximum_priority : int -> string * Types.value

  val declare :
    _ Channel.t ->
    ?durable:bool ->
    ?exclusive:bool ->
    ?auto_delete:bool ->
    ?passive:bool -> ?arguments:Types.table -> string -> t

  val declare_anonymous :
    _ Channel.t ->
    ?durable:bool ->
    ?exclusive:bool ->
    ?auto_delete:bool ->
    ?passive:bool -> ?arguments:Types.table -> unit -> t

  val bind : _ Channel.t -> t -> 'a Exchange.t -> 'a
  val unbind : _ Channel.t -> t -> 'a Exchange.t -> 'a
  val purge : _ Channel.t -> t -> int
  val delete : _ Channel.t -> ?if_unused:bool -> ?if_empty:bool -> t -> int
  val name : t -> string
  val fake : string -> t


  type consumer
  val cancel_consumer : _ Channel.t -> consumer -> unit

  val consume :  _ Channel.t -> ?no_local:bool -> ?no_ack:bool -> ?exclusive:bool -> id:string -> t -> (consumer * (unit -> (Message.t)))
  val publish : 'a Channel.t -> t -> ?mandatory:bool -> Message.content -> 'a
  val get : 'a Channel.t -> no_ack:bool -> t -> Message.t option

  module Raw : sig
    val consume :  _ Channel.t -> ?no_local:bool -> ?no_ack:bool -> ?exclusive:bool -> id:string -> t -> (consumer * (unit -> (Message.t_raw)))
    val publish : 'a Channel.t -> t -> ?mandatory:bool -> Message.content_raw -> 'a
    val get : 'a Channel.t -> no_ack:bool -> t -> Message.t_raw option
  end

end
