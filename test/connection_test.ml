open Amqp_client_eio

let port =
  Sys.getenv_opt "AMQP_PORT"
  |> Option.map int_of_string

let test_amqp env =
  Eio.Switch.run (fun sw ->
    let connection = Connection.init ~sw ~env ~id:"Test" ?port "127.0.0.1" in
    Eio.traceln "Connection created";
    Connection.close connection "Closed by user";
    ()
  )

let () =
  Test_lib.run_with_timeout test_amqp
