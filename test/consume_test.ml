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
    let queue = Queue.declare channel "consume_test" in
    Eio.traceln "Queue created";
    let _purge = Queue.purge channel queue in

    let count = Atomic.make 0 in
    let consumer, stream = Queue.consume channel queue ~id:"Test consumer" in

    (* Start consumption *)
    Eio.Fiber.fork ~sw (fun () ->
      let rec loop () =
        let deliver, (_content, body) = Stream.receive stream in
        Eio.traceln "Received message: %s" body;
        Message.ack channel deliver;
        Atomic.incr count;
        loop ()
      in
      try
        loop ()
      with
      | _ -> ()
    );
    Queue.publish channel queue (Message.make "Msg1") |> assert_ok;
    Queue.publish channel queue (Message.make "Msg2") |> assert_ok;
    Queue.publish channel queue (Message.make "Msg3") |> assert_ok;

    (* Sleep a bit! *)
    Test_lib.sleep env 1.0;

    Queue.cancel_consumer channel consumer;
    assert (Atomic.get count = 3);
    let deleted_messages = Queue.delete channel queue in
    Eio.traceln "Queue deleted with %d messages" deleted_messages;
    assert (deleted_messages = 0);
    Connection.close connection "Closed by me";
    ()
  )

let () =
  Test_lib.run_with_timeout test_amqp
