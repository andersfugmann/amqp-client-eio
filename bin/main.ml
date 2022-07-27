(** Simple test client *)
open Amqp_client_eio

let run env =
  Eio.Switch.run (fun sw ->
    let connection = Connection.init ~sw ~env ~id:"Test" "127.0.0.1" in
    Printf.printf "Connection created\n%!";
    let _channel = Channel.init ~sw connection Channel.With_confirm in
    Printf.printf "Channel created\n%!";
    ()
  )

let () =
  Eio_main.run run
