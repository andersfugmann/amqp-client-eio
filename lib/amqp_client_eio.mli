module Types : sig
  type message_id = Types.message_id = { class_id : int; method_id : int; }
  module Message_id = Types.Message_id
  module Frame_type = Types.Frame_type
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
  val print_type : string -> value -> unit
  type nonrec int = int
  type nonrec string = string
  type nonrec bool = bool
end


module Connection : sig
  exception Closed of string
  exception Break_loop
  module Credentials = Connection.Credentials
  type t
  val init :
    sw:Eio__core.Switch.t ->
    env:< clock : #Eio.Time.clock; net : #Eio.Net.t; .. > ->
    id:string ->
    ?virtual_host:string ->
    ?heartbeat:int ref ->
    ?max_frame_size:int ->
    ?max_stream_length:int ->
    ?credentials:Credentials.t -> ?port:int -> string -> t
end

module Channel : sig
  type 'a t
  type 'a confirms
  val no_confirm : [ `Ok ] confirms
  val with_confirm : [ `Ok | `Rejected ] confirms
  exception Closed of Types.string
  exception Channel_closed of Spec.Channel.Close.t
  type 'a with_confirms = 'a confirms -> 'a t constraint 'a = [< `Ok | `Rejected > `Ok ]
  val init : sw:Eio.Switch.t -> Connection.t -> [< `Ok | `Rejected > `Ok ] with_confirms
end

module Message : sig
  val string_header : 'a -> string -> 'a * Types.value
  val int_header : 'a -> int -> 'a * Types.value
  type content = Spec.Basic.Content.t * string
  type t = Message.t
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
  val ack : 'a Channel.t -> ?multiple:bool -> t -> unit
  val reject : 'a Channel.t -> ?multiple:bool -> requeue:bool -> t -> unit
  val recover : 'a Channel.t -> requeue:bool -> unit
end

module Exchange : sig
  type 'a t
  type 'a exchange_type
  val direct_t : [ `Queue of string ] exchange_type
  val fanout_t : unit exchange_type
  val topic_t : [ `Topic of string ] exchange_type
  val match_t : [ `Headers of Types.header list ] exchange_type
  val default : [ `Queue of string ] t
  val amq_direct : [ `Queue of string ] t
  val amq_fanout : unit t
  val amq_topic : [ `Topic of string ] t
  val amq_match : [ `Headers of Types.header list ] t
  val declare :
    ?passive:bool ->
    ?durable:bool ->
    ?auto_delete:bool ->
    ?internal:bool ->
    'b Channel.t ->
    'a exchange_type -> ?arguments:Types.table -> string -> 'a t
  val delete : 'a Channel.t -> ?if_unused:bool -> 'b t -> unit
  val bind : 'b Channel.t -> destination:'c t -> source:'a t -> 'a -> unit
  val unbind : 'b Channel.t -> destination:'c t -> source:'a t -> 'a -> unit
  val publish :
    _ t -> 'a Channel.t -> ?mandatory:bool -> routing_key:string -> Message.content -> 'a
end


module Queue : sig
  type t = string
  val message_ttl : int -> string * Types.value
  val auto_expire : int -> string * Types.value
  val max_length : int -> string * Types.value
  val max_length_bytes : int -> string * Types.value
  val dead_letter_exchange : string -> string * Types.value
  val dead_letter_routing_key : string -> string * Types.value
  val maximum_priority : int -> string * Types.value
  val declare :
    'a Channel.t ->
    ?durable:bool ->
    ?exclusive:bool ->
    ?auto_delete:bool ->
    ?passive:bool -> ?arguments:Types.table -> string -> string

  val declare_anonymous :
    'a Channel.t ->
    ?durable:bool ->
    ?exclusive:bool ->
    ?auto_delete:bool ->
    ?passive:bool -> ?arguments:Types.table -> unit -> string

  val get : no_ack:bool -> 'a Channel.t -> string -> Cstruct.t list option
end
