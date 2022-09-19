let run_with_timeout ?(timeout = 5.0) f =
  Eio_main.run (fun env ->
    Eio.Fiber.first
      (fun () -> Eio.Time.sleep (Eio.Stdenv.clock env) timeout; failwith "Timeout")
      (fun () -> f env)
  )
