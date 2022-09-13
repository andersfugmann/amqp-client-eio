open !StdLabels
module Queue = Stdlib.Queue

let failwith_f fmt = Printf.ksprintf (fun s -> failwith s) fmt
let log fmt = Printf.printf (fmt ^^ "\n%!")

module Promise = struct
  type 'a t = ('a, exn) result Eio.Promise.t
  type 'a u = ('a, exn) result Eio.Promise.u

  let create = Eio.Promise.create
  let await = Eio.Promise.await_exn
  let resolve_ok = Eio.Promise.resolve_ok
  let resolve_exn = Eio.Promise.resolve_error
  let is_resolved = Eio.Promise.is_resolved
end

(** Extension to Eio.Stream, which allows closing the stream.
    A closed stream might still hold messages. Once the last message is taken off
    a close stream, the stream will raise a user defined exception.

    Posting to or closing a closed stream will raise same exception.

    {1 note} This is a far from perfect implementation:
    - Closing a stream must wait until there is room in the stream.

    This should be re-written to use a promise for indicating that the channel has been closed.
    This way any pending publishers can be cancelled. Still need to figure out how reliably signal consumers.
    Well these could wait also wait on the promise. If the promise is ready, they check if there are more messages.
    ( Could start with a nonblocking read before checking ). Would be so easy if we had a switch (and could fork).
    We only want one message to be posted.
*)
module Stream = struct
  open Eio.Private
  type 'a item = ('a, exn) result
  type 'a t = {
    mutex : Mutex.t;
    id : Ctf.id;
    capacity : int;
    readers : 'a item Waiters.t;
    writers : (unit, exn) result Waiters.t;
    items : 'a Queue.t;
    mutable closed : exn option;
  }

  let with_mutex t f =
    Mutex.lock t.mutex;
    match f () with
    | x -> Mutex.unlock t.mutex; x
    | exception ex -> Mutex.unlock t.mutex; raise ex

  let create ?(capacity = Int.max_int) () =
    assert (capacity >= 0);
    let id = Ctf.mint_id () in
    Ctf.note_created id Ctf.Stream;
    {
      mutex = Mutex.create ();
      id;
      capacity;
      items = Queue.create ();
      readers = Waiters.create ();
      writers = Waiters.create ();
      closed = None;
    }

  (** Push a message onto the stream.
      @raise Closed if the stream has been closed *)
  let send t item =
    Mutex.lock t.mutex;
    match Waiters.wake_one t.readers (Ok item) with
    | `Ok -> Mutex.unlock t.mutex
    | `Queue_empty ->
      (* Raise if the stream has been closed *)
      let () = match t.closed with
        | Some exn ->
          Mutex.unlock t.mutex;
          raise exn
        | None -> ()
      in
      (* No-one is waiting for an item. Queue it. *)
      if Queue.length t.items < t.capacity then (
        Queue.add item t.items;
        Mutex.unlock t.mutex
      ) else (
        (* The queue is full. Wait for our turn first. *)
        Suspend.enter_unchecked @@ fun ctx enqueue ->
        Waiters.await_internal ~mutex:(Some t.mutex) t.writers t.id ctx (fun r ->
          (* This is called directly from [wake_one] and so we have the lock.
             We're still running in [wake_one]'s domain here. *)
          if Result.is_ok r then (
            (* We get here immediately when called by [take], either:
               1. after removing an item, so there is space, or
               2. if [capacity = 0]; [take] will immediately remove the new item. *)
            Queue.add item t.items;
          );
          let r = match r with
            | Error _ as e -> e
            | Ok (Ok r) -> Ok r
            | Ok (Error _ as e) -> e
          in
          enqueue r
        )
      )

  (** Pop the first element of the stream.
      @raise exception if the stream has been closed, and there are no more message on the stream
  *)
  let receive t =
    Mutex.lock t.mutex;
    match Queue.take_opt t.items with
    | None ->
      (* There aren't any items, so we probably need to wait for one.
         However, there's also the special case of a zero-capacity queue to deal with.
         [is_empty writers || capacity = 0] *)
      begin match Waiters.wake_one t.writers (Ok ()) with
      | `Queue_empty -> begin
          (* Don't sleep if the queue has been closed *)
          let () = match t.closed with
            | Some exn ->
              Mutex.unlock t.mutex;
              raise exn
            | None -> ()
          in
          match Waiters.await ~mutex:(Some t.mutex) t.readers t.id with
          | Ok x -> x
          | Error exn -> raise exn
        end
      | `Ok ->
        (* [capacity = 0] (this is the only way we can get waiters and no items).
           [wake_one] has just added an item to the queue; remove it to restore
           the invariant before closing the mutex. *)
        let x = Queue.take t.items in
        Mutex.unlock t.mutex;
        x
      end
    | Some v ->
      (* If anyone was waiting for space, let the next one go.
         [is_empty writers || length items = t.capacity - 1] *)
      begin match Waiters.wake_one t.writers (Ok ()) with
      | `Ok                     (* [length items = t.capacity] again *)
      | `Queue_empty -> ()      (* [is_empty writers] *)
      end;
      Mutex.unlock t.mutex;
      v

  (** Close the stream.
      Reading from a closed stream will raise the close exception once empty, if the stream was closed
      with [notify_consumers]. Closing an already closed stream does nothing (and will not update the close reason).

      @param message Post a message onto the queue after its closed, guaranteeing that its the last message on the queue (weak guarantee). Note that this might block until the stream has room

  *)
  let close ?message t reason =
    Mutex.lock t.mutex;
    t.closed <- Some reason;
    (* Add the last message to the queue if needed *)
    let () = match message with
      | Some item -> begin
          match Waiters.wake_one t.readers (Ok item) with
          | `Ok -> ()
          | `Queue_empty ->
            Queue.add item t.items;
        end
      | None -> ()
    in
    Waiters.wake_all t.writers (Error reason);
    Waiters.wake_all t.readers (Error reason);
    Mutex.unlock t.mutex

  let is_empty t =
    Mutex.lock t.mutex;
    let res = Queue.is_empty t.items in
    Mutex.unlock t.mutex;
    res

  let is_closed { closed; _ } =
    Option.is_some closed

end

module Mutex = struct
  include Eio.Mutex

  let with_lock l f =
    lock l;
    try
      let res = f () in
      unlock l;
      res
    with e ->
      unlock l;
      raise e
end
