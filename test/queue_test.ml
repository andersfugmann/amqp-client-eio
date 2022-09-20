(** Simple test client *)
open Amqp_client_eio

let port =
  Sys.getenv_opt "AMQP_PORT"
  |> Option.map int_of_string


let assert_ok = function
  | `Ok -> ()
  | `Rejected -> failwith "Message rejected"

let test_amqp env =
  Eio.Switch.run (fun sw ->
    let connection = Connection.init ~sw ~env ~id:"Test" ?port "127.0.0.1" in
    Eio.traceln "Connection created";
    let channel = Channel.init ~sw connection Channel.with_confirm in
    Eio.traceln "Channel created";
    let queue = Queue.declare channel "queue_test" in
    Eio.traceln "Queue created";
    let _purge = Queue.purge channel queue in
    (* Lets create a couple of message *)
    Queue.publish channel queue (Message.make "Msg1") |> assert_ok;
    Queue.publish channel queue (Message.make "Msg2") |> assert_ok;
    Queue.publish channel queue (Message.make "Msg3") |> assert_ok;

    let deleted_messages = Queue.delete channel queue in
    Eio.traceln "Queue deleted with %d messages" deleted_messages;
    assert (deleted_messages = 3);
    Connection.close connection "Closed by me";
    ()
  )

let () =
  Test_lib.run_with_timeout test_amqp
