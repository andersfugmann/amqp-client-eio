(** Simple test client *)
open Amqp_client_eio

(* Use a different domain! *)
let rec send channel queue = function
  | 0 -> ()
  | n ->
    Queue.publish channel ~mandatory:true queue (Message.make (string_of_int n));
    send channel queue (n - 1)

let send ~sw connection stream =
  let channel = Channel.init ~sw connection Channel.no_confirm in
  let queue = Queue.declare channel "flow" in
  let rec loop () =
    match Eio.Stream.take stream with
    | `Send n ->
      Eio.traceln "Got send %d" n;
      send channel queue n;
      loop ()
    | `Stop -> ()
  in
  loop ()

let rec consume stream deliver x n =
  let (_, (_, _body)) = deliver () in
  match n with
  | 0 ->
    Eio.Stream.add stream `Stop;
  | n when n mod x = 0 ->
    Eio.Stream.add stream (`Send x);
    Eio.traceln "%d" n;
    consume stream deliver x (n - 1)
  | n ->
    consume stream deliver x (n - 1)


let test_amqp env =
  Eio.Switch.run (fun sw ->
    let connection = Connection.init ~sw ~env ~id:"Test" "127.0.0.1" in
    Eio.traceln "Connection created";
    let channel = Channel.init ~sw connection Channel.no_confirm in
    let queue = Queue.declare channel "flow" in
    let _ = Queue.purge channel queue in
    Eio.traceln "Channel created";
    let stream = Eio.Stream.create 10 in
    Eio.Stream.add stream (`Send 10000);
    Eio.Fiber.fork ~sw (fun () ->
      Eio.Domain_manager.run (Eio.Stdenv.domain_mgr env) (fun () -> Eio.Switch.run (fun sw -> send ~sw connection stream))
    );
    let (_consumer, deliver) = Queue.consume channel queue ~no_ack:true ~id:"flow consumer" in
    Eio.Domain_manager.run (Eio.Stdenv.domain_mgr env) (fun () -> consume stream deliver 10000 1_000_000);
    Connection.close connection "Closed by me";
    ()
  )

(* Create a product on one domain and a consumer on another domain *)

let () =
  Eio_main.run test_amqp
  (* No fibers can block when the switch is cancelled *)
