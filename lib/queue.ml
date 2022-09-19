open !StdLabels
open Spec.Queue

type t = string

let message_ttl v = "x-message-ttl", Types.VLonglong v
let auto_expire v = "x-expires", Types.VLonglong v
let max_length v = "x-max-length", Types.VLonglong v
let max_length_bytes v = "x-max-length-bytes", Types.VLonglong v
let dead_letter_exchange v = "x-dead-letter-exchange", Types.VLongstr v
let dead_letter_routing_key v = "x-dead-letter-routing-key", Types.VLongstr v
let maximum_priority v = "x-max-priority", Types.VLonglong v

let declare ?(durable=false) ?(exclusive=false) ?(auto_delete=false) ?(passive=false) ?(arguments=[]) channel name =
  let Declare_ok.{ queue; message_count; consumer_count } =
    Declare.client_request channel.Channel.service
      ~queue:name ~passive ~durable ~exclusive ~auto_delete ~no_wait:false ~arguments ()
  in
  assert (queue = name);
  Eio.traceln "Queue declared: Messages: %d. Consumers: %d" message_count consumer_count;
  name

let declare_anonymous ?(durable=false) ?(exclusive=false) ?(auto_delete=false) ?(passive=false) ?(arguments=[]) channel =
  let Declare_ok.{ queue; message_count; consumer_count } =
    Declare.client_request channel.Channel.service
      ~queue:"" ~passive ~durable ~exclusive ~auto_delete ~no_wait:false ~arguments ()
  in
  Eio.traceln "Anonymous Queue declared: %s Messages: %d. Consumers: %d" queue message_count consumer_count;
  queue

let get ~no_ack channel name =
  match Spec.Basic.Get.client_request channel.Channel.service ~queue:name ~no_ack () with
  | `Get_empty _ -> None
  | `Get_ok (_result, (_content, data)) -> Some data

(** Publish a message directly to a queue *)
let publish t channel ?mandatory message =
  Exchange.publish Exchange.default channel ?mandatory
    ~routing_key:t
    message


type consumer = Channel.consumer_tag
(* Closing the stream will cancel consumption *)
let consume queue ?(no_local=false) ?(no_ack=false) ?(exclusive=false) ~id channel =
  (* Need a sequential id *)
  let receive_stream = Utils.Stream.create () in
  let consumer_tag = Channel.register_consumer channel ~receive_stream ~id in
  let res = Spec.Basic.Consume.client_request channel.service ~queue ~consumer_tag ~no_local ~no_ack ~exclusive ~no_wait:false ~arguments:[] () in
  assert (res.consumer_tag = consumer_tag);
  (consumer_tag, receive_stream)

let cancel_consumer consumer_tag channel =
  let res = Spec.Basic.Cancel.client_request channel.Channel.service ~consumer_tag ~no_wait:false () in
  assert (res.consumer_tag = consumer_tag);
  Channel.deregister_consumer channel ~consumer_tag

let bind: type a. t -> _ Channel.t -> a Exchange.t -> a = fun queue channel exchange ->
  let bind ?(routing_key="") ?(arguments=[]) () =
    Bind.client_request channel.Channel.service ~queue ~exchange:exchange.Exchange.name ~routing_key ~no_wait:false ~arguments ()
  in
  match exchange.exchange_type with
  | Direct -> fun ~queue -> bind ~routing_key:queue ()
  | Fanout -> bind ()
  | Topic -> fun ~topic -> bind ~routing_key:topic ()
  | Match -> fun ~headers -> bind ~arguments:headers ()

let unbind: type a. t -> _ Channel.t -> a Exchange.t -> a = fun queue channel exchange ->
  let unbind ?(routing_key="") ?(arguments=[]) () =
    Unbind.client_request channel.Channel.service ~queue ~exchange:exchange.Exchange.name ~routing_key ~arguments ()
  in
  match exchange.exchange_type with
  | Direct -> fun ~queue -> unbind ~routing_key:queue ()
  | Fanout -> unbind ()
  | Topic -> fun ~topic -> unbind ~routing_key:topic ()
  | Match -> fun ~headers -> unbind ~arguments:headers ()

(** Purge the queue *)
let purge queue channel =
  let Purge_ok.{ message_count } =
    Purge.client_request channel.Channel.service ~queue ~no_wait:false ()
  in
  message_count

(** Delete the queue. *)
let delete ?(if_unused=false) ?(if_empty=false) queue channel =
  let Delete_ok.{ message_count } = Delete.client_request channel.Channel.service ~queue ~if_unused ~if_empty ~no_wait:false () in
  message_count

(** Name of the queue *)
let name t = t

(** Construct a queue without any validation *)
let fake name = name
