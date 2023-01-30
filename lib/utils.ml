open !StdLabels
module Queue = Stdlib.Queue

let failwith_f fmt = Printf.ksprintf (fun s -> failwith s) fmt
let log fmt = Eio.traceln fmt

module Condition = struct
  open Stdlib
  open Eio.Private
  type t = {
    waiters: unit Waiters.t;
    id: Ctf.id
  }

  let create () = {
    waiters = Waiters.create ();
    id = Ctf.mint_id ();
  }

  let await t mutex =
    match Waiters.await ~mutex:(Some mutex) t.waiters t.id with
    | ()           -> Mutex.lock mutex
    | exception ex -> Mutex.lock mutex; raise ex

  let broadcast t =
    Waiters.wake_all t.waiters ()

  let _signal t =
    Waiters.wake_one t.waiters ()
end

(* A Simpler solution would be an unbounded stream.
   - That is not an option.

   - Closing a stream is a signalling. If a consumer closes a stream, we want to
   poster to understand this.... Why not wrap the existing stream. That should be simple.

   We have atomic operations - that should work to create a mutable exlusive region.

   Pending publishers are hard to cancel. Which is needed if the channel is full.
   - But this means that if the stream is closed by a consumer it _MUST_ be flushed.

   Close posts a close message to the stream (in a fiber)
   Flush just reads until receiving the close message

   - All consumers will see the close message and redeliver
   - All publishers will succeed.

   New publishes will be stopped.

   Ok. Thats a simple solution which should not require a complete copy of
   the existing implementation (The eio implementation is not lock free for capacity > 0)

   - The condition to signal once the queue is empty or full is not needed.
   - Because we cannot use it. Receiving messages with ack it should be done
   - setting prefetch.
   - If we want to signal full then we could to a try_post.

   - We need to extend the implementation with add_nonblocking and add ~force.
   - Send patches upstream for this.



   Alternative:
   Create a wrapper around the stream implementation:
   Closed can be checked before posting.
   That region must be guarded - so we dont race.
   Does this mean that we will always take a lock.
   Damn! If post blocks then we cannot release the lock!
   We can approximate, but that will allow messages to arrive after the close message.
   - However that may not be a problem. But the close message must not be received before
   - all other messages has been consumed - and thats not guaranteed!

*)

(** Extension to Eio.Stream, which allows closing the stream.

    Closing a stream will allow all pending publishers to complete.
    All pending receivers (=> queue is initially empty) will receive an exception
    Posting to or closing a closed stream will raise same exception.
*)
module Stream = struct
  open Eio.Private
  type 'a t = {
    mutex : Mutex.t;
    id : Ctf.id;

    capacity : int;               (* [capacity > 0] *)
    items : 'a Queue.t;

    (* Readers suspended because [items] is empty. *)
    readers : ('a, exn) result Waiters.t;

    (* Writers suspended because [items] is at capacity. *)
    writers : unit Waiters.t;

    mutable closed : exn option;
  }

  let with_mutex t f =
    Mutex.lock t.mutex;
    match f () with
    | x -> Mutex.unlock t.mutex; x
    | exception ex -> Mutex.unlock t.mutex; raise ex

  (* Invariants *)
  let _validate t =
    with_mutex t @@ fun () ->
    assert (Queue.length t.items <= t.capacity);
    assert (Waiters.is_empty t.readers || Queue.is_empty t.items);
    assert (Waiters.is_empty t.writers || Queue.length t.items = t.capacity)

  let create ?(capacity = Int.max_int) () =
    assert (capacity > 0);
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
      @raise Closed if the stream has been closed
      @param force if true, ignore max_capacity and send will not block
  *)
  let add ?(force=false) t item =
    Mutex.lock t.mutex;
    match Waiters.wake_one t.readers (Ok item) with
    | `Ok -> Mutex.unlock t.mutex
    | `Queue_empty ->
      let () = match t.closed with
        | Some exn ->
          Mutex.unlock t.mutex;
          raise exn
        | None -> ()
      in
      (* No-one is waiting for an item. Queue it. *)
      if Queue.length t.items < t.capacity || force then (
        Queue.add item t.items;
        Mutex.unlock t.mutex
      ) else (
        (* The queue is full. Wait for our turn first. *)
        Suspend.enter_unchecked @@ fun ctx enqueue ->
        Waiters.await_internal ~mutex:(Some t.mutex) t.writers t.id ctx (fun r ->
            (* This is called directly from [wake_one] and so we have the lock.
               We're still running in [wake_one]'s domain here. *)
            if Result.is_ok r then (
              (* We get here immediately when called by [take], after removing an item,
                 so there is space *)
              Queue.add item t.items;
            );
            enqueue r
          )
      )
  let send = add

  let take t =
    Mutex.lock t.mutex;
    match Queue.take_opt t.items with
    | None ->
      let () = match t.closed with
        | Some exn ->
          Mutex.unlock t.mutex;
          raise exn
        | None -> ()
      in
      begin
        (* There aren't any items, so we need to wait for one. *)
        match Waiters.await ~mutex:(Some t.mutex) t.readers t.id with
        | Ok item -> item
        | Error exn -> raise exn
      end
    | Some v ->
      (* If anyone was waiting for space, let the next one go.
         [is_empty writers || length items = t.capacity - 1] *)
      begin match Waiters.wake_one t.writers () with
        | `Ok                     (* [length items = t.capacity] again *)
        | `Queue_empty -> ()      (* [is_empty writers] *)
      end;
      Mutex.unlock t.mutex;
      v

  let receive = take

  let take_nonblocking t =
    Mutex.lock t.mutex;
    let () = match t.closed with
      | Some exn ->
        Mutex.unlock t.mutex;
        raise exn
      | None -> ()
    in

    match Queue.take_opt t.items with
    | None -> Mutex.unlock t.mutex; None (* There aren't any items. *)
    | Some v ->
      (* If anyone was waiting for space, let the next one go.
         [is_empty writers || length items = t.capacity - 1] *)
      begin match Waiters.wake_one t.writers () with
        | `Ok                     (* [length items = t.capacity] again *)
        | `Queue_empty -> ()      (* [is_empty writers] *)
      end;
      Mutex.unlock t.mutex;
      Some v

  let length t =
    Mutex.lock t.mutex;
    let len = Queue.length t.items in
    Mutex.unlock t.mutex;
    len

  let receive_all t =
    let rec loop () =
      match Queue.take_opt t.items with
      | None -> []
      | Some i -> i :: loop ()
    in
    let get_all_items () =
      Mutex.lock t.mutex;
      Waiters.wake_all t.writers ();
      let items = loop () in
      Mutex.unlock t.mutex;
      items
    in
    let item = receive t in
    let items = get_all_items () in
    item :: items

  let close ?message t reason =
    Mutex.lock t.mutex;
    t.closed <- Some reason;
    (* Wake all writers. They will just publish the message to the queue *)
    Waiters.wake_all t.writers ();
    (* If there were pending writers => no pending readers *)
    (* If there were pending readers => no pending writers *)

    let () = match message with
      | Some item -> begin
          match Waiters.wake_one t.readers (Ok item) with
          | `Ok -> ()
          | `Queue_empty -> Queue.add item t.items;
        end
      | None -> ()
    in
    (* If there are pending readers, that mean that the queue is empty.
       Since we will not allow new publishes, we signal close to them
    *)
    Waiters.wake_all t.readers (Error reason);
    Mutex.unlock t.mutex;
    ()

  let is_empty t =
    Mutex.lock t.mutex;
    let res = Queue.is_empty t.items in
    Mutex.unlock t.mutex;
    res

  let is_closed t  =
    Mutex.lock t.mutex;
    let res = Option.is_some t.closed in
    Mutex.unlock t.mutex;
    res

  let dump f t =
    Fmt.pf f "<Locking stream: %d/%d items>" (length t) t.capacity
end

open Eio
module Promise = struct
  type 'a t = ('a, exn) result Promise.t
  type 'a u = ('a, exn) result Promise.u

  let create = Promise.create
  let await = Promise.await_exn
  let resolve_ok = Promise.resolve_ok
  let resolve_exn = Promise.resolve_error
  let is_resolved = Promise.is_resolved
end

module Mutex = struct
  include Mutex
  let with_lock l f =
    Mutex.lock l;
    try
      let res = f () in
      Mutex.unlock l;
      res
    with e ->
      Mutex.unlock l;
      raise e
end

module Monitor = struct
  type t = Mutex.t * Condition.t

  (** Create a new monitor *)
  let init () = (Mutex.create (), Condition.create ())

  (** Update mutable structure state *)
  let update (mutex, condition) update state =
    let res = Mutex.with_lock mutex @@ fun () -> update state in
    Condition.broadcast condition;
    res

  (** Wait until the predicate becomes true on on the mutable shared state [state].
      All updates to state must be made through call to update *)
  let wait (mutex, condition) ~predicate state =
    let rec wait () =
      match predicate state with
      | true -> ()
      | false ->
        Condition.await condition mutex;
        wait ()
    in
    match predicate state with
    | false -> Mutex.with_lock mutex @@ wait
    | true -> ()
end
