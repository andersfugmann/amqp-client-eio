(** Simple test client *)
open Amqp_client_eio
let test_amqp env =
  Eio.Switch.run (fun sw ->
    let connection = Connection.init ~sw ~env ~id:"Test" "127.0.0.1" in
    Eio.traceln "Connection created";
    let channel = Channel.init ~sw connection Channel.no_confirm in
    Eio.traceln "Channel created";
    let _queue = Queue.declare channel "test_queue" in
    Eio.traceln "Queue created";
    let () = Exchange.publish Exchange.default channel ~mandatory:true ~routing_key:"test_queue" (Message.make "1234567") in
    Eio.Time.sleep (Eio.Stdenv.clock env) 0.1;
    Connection.close connection "Closed by me";
    ()
  )

let () =
  Eio_main.run test_amqp
  (* No fibers can block when the switch is cancelled *)
