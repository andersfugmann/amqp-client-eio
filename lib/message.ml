open !StdLabels
open Types


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

let make
    ?(content_type:string option)
    ?(content_encoding: string option)
    ?(headers: Types.table option)
    ?(delivery_mode: int option)
    ?(priority: int option)
    ?(correlation_id: string option)
    ?(reply_to: string option)
    ?(expiration: int option)
    ?(message_id: string option)
    ?(timestamp: int option)
    ?(amqp_type: string option)
    ?(user_id: string option)
    ?(app_id: string option)
    body : content =


  ({Spec.Basic.Content.
     content_type;
     content_encoding;
     headers;
     delivery_mode;
     priority;
     correlation_id;
     reply_to;
     expiration = Option.map string_of_int expiration;
     message_id = (message_id : string option);
     timestamp;
     amqp_type;
     user_id;
     app_id
   }, body)

let ack: _ Channel.t -> ?multiple:bool -> deliver -> unit = fun channel ?(multiple=false) t ->
  Spec.Basic.Ack.client_request channel.service ~delivery_tag:t.delivery_tag ~multiple ()

let reject: _ Channel.t -> ?multiple:bool -> requeue:bool -> deliver -> unit = fun channel ?(multiple=false) ~requeue t ->
  Spec.Basic.Nack.client_request channel.service ~delivery_tag:t.delivery_tag ~multiple ~requeue ()

let recover: _ Channel.t -> requeue:bool -> unit = fun channel ~requeue ->
  Spec.Basic.Recover.client_request channel.service ~requeue ()
