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

let declare channel ?(durable=false) ?(exclusive=false) ?(auto_delete=false) ?(passive=false) ?(arguments=[]) name =
  let Declare_ok.{ queue; message_count; consumer_count } =
    Declare.client_request channel.Channel.service
      ~queue:name ~passive ~durable ~exclusive ~auto_delete ~no_wait:false ~arguments ()
  in
  assert (queue = name);
  Eio.traceln "Queue declared: Messages: %d. Consumers: %d" message_count consumer_count;
  name

let declare_anonymous channel ?(durable=false) ?(exclusive=false) ?(auto_delete=false) ?(passive=false) ?(arguments=[]) () =
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

(*
type 'a consumer = { channel: 'a Channel.t;
                     tag: string;
                     writer: Message.t Pipe.Writer.t }

(** Consume message from a queue. *)
let consume ~id ?(no_local=false) ?(no_ack=false) ?(exclusive=false)
    ?on_cancel channel t =
  let open Spec.Basic in
  let (reader, writer) = Pipe.create () in
  let consumer_tag = Printf.sprintf "%s.%s" (Channel.Internal.unique_id channel) id
  in
  let on_cancel () =
    Pipe.close_without_pushback writer;
    match on_cancel with
    | Some f -> f ()
    | None -> raise (Types.Consumer_cancelled consumer_tag)
  in

  let to_writer (deliver, header, body) =
    { Message.delivery_tag = deliver.Deliver.delivery_tag;
      Message.redelivered = deliver.Deliver.redelivered;
      Message.exchange = deliver.Deliver.exchange;
      Message.routing_key = deliver.Deliver.routing_key;
      Message.message = (header, body) }
    |> Pipe.write_without_pushback writer
  in
  let req = { Consume.queue=t.name;
              consumer_tag;
              no_local;
              no_ack;
              exclusive;
              no_wait = false;
              arguments = [];
            }
  in
  let var = Ivar.create () in
  let on_receive consume_ok =
    Channel.Internal.register_consumer_handler channel consume_ok.Consume_ok.consumer_tag to_writer on_cancel;
    Ivar.fill var consume_ok
  in
  let read = snd Consume_ok.Internal.read in
  read ~once:true on_receive (Channel.channel channel);

  Consume.Internal.write (Channel.channel channel) req >>= fun () ->
  Ivar.read var >>= fun rep ->
  let tag = rep.Consume_ok.consumer_tag in
  return ({ channel; tag; writer }, reader)

let cancel consumer =
  let open Spec.Basic in
  Cancel.request (Channel.channel consumer.channel) { Cancel.consumer_tag = consumer.tag; no_wait = false } >>= fun _rep ->
  Channel.Internal.deregister_consumer_handler consumer.channel consumer.tag;
  Pipe.close consumer.writer
*)

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
