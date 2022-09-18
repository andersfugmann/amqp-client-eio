(** Simple test client *)
open Amqp_client_eio

let test_amqp env =
  Eio.Switch.run (fun sw ->
    Eio.Fiber.fork ~sw (fun () -> Eio.Time.sleep (Eio.Stdenv.clock env) 2.0; failwith "Timeout");
    let connection = Connection.init ~sw ~env ~id:"Test" "127.0.0.1" in
    Eio.traceln "Connection created";
    let _channel = Channel.init ~sw connection Channel.no_confirm in
    Eio.traceln "Channel created";
    Connection.close connection "Closed by me";
    ()
  )

let () =
  Eio_main.run test_amqp
  (* No fibers can block when the switch is cancelled *)
