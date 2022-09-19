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
    let queue = Queue.declare channel "test_queue" in
    Eio.traceln "Queue created";
    let _purge = Queue.purge queue channel in
    (* Lets create a couple of message *)
    Queue.publish queue channel (Message.make "Msg1");
    Queue.publish queue channel (Message.make "Msg2");
    Queue.publish queue channel (Message.make "Msg3");

    (* Need to ensure that all message are flushed *)
    Eio.Time.sleep (Eio.Stdenv.clock env) 1.0;

    let deleted_messages = Queue.delete queue channel in
    Eio.traceln "Queue deleted with %d messages" deleted_messages;
    assert (deleted_messages = 3);
    Connection.close connection "Closed by me";
    ()
  )

let () =
  Test_lib.run_with_timeout test_amqp
