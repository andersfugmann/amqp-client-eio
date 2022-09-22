(** Simple test client *)
open Amqp_client_eio

let rec send_receive channel queue = function
  | 0 -> ()
  | n ->
    let _ = Queue.publish channel ~mandatory:true queue (Message.make (string_of_int n)) in
    let (_, (_, body)) = Queue.get channel ~no_ack:true queue |> Option.get in
    (* Eio.traceln "received message: '%s' = %d = %d" body (try int_of_string body with _ -> -1) n; *)
    assert (int_of_string body = n);
    send_receive channel queue (n - 1)

let test_amqp env =
  Eio.Switch.run (fun sw ->
    let connection = Connection.init ~sw ~env ~id:"Test" "127.0.0.1" in
    Eio.traceln "Connection created";
    let channel = Channel.init ~sw connection Channel.with_confirm in
    Eio.traceln "Channel created";
    let queue = Queue.declare channel "stress" in
    let _ = Queue.purge channel queue in
    Eio.traceln "Queue created";
    send_receive channel queue 1000;
    Connection.close connection "Closed by me";
    ()
  )

(* Create a product on one domain and a consumer on another domain *)

let () =
  Eio_main.run test_amqp
  (* No fibers can block when the switch is cancelled *)
