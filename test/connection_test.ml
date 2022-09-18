open Amqp_client_eio

let test_amqp env =
  Eio.Switch.run (fun sw ->
    let connection = Connection.init ~sw ~env ~id:"Test" "127.0.0.1" in
    Eio.traceln "Connection created";
    Connection.close connection "Closed by user";
    ()
  )

let () =
  Eio_main.run test_amqp
