(** Simple test client *)
open Amqp_client_eio

let port =
  Sys.getenv_opt "AMQP_PORT"
  |> Option.map int_of_string

let test_amqp env =
  Eio.Switch.run (fun sw ->
    let connection = Connection.init ~sw ~env ~id:"Test" ?port "127.0.0.1" in
    Eio.traceln "Connection created";
    let channel = Channel.init ~sw connection Channel.no_confirm in
    Eio.traceln "Channel created";
    let _queue = Queue.declare channel "test_queue" in
    Eio.traceln "Queue created";
    Connection.close connection "Closed by me";
    ()
  )

let () =
  Test_lib.run_with_timeout test_amqp
