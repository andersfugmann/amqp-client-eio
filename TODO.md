Define one exception type with a polymorphic variant for all errors
 - [x] Add an apply with named arguments function to genspec.ml (reverse of init)
 - [x] Hide all defs and construct {client,server}_{request,reply} function to guarantee correct usage.
 - [x] Create a generic framework for communicating (used by both the
       connection and channel modules)
 - [x] Add type constraints to Protocol.Spec.def and
       Protocol.Content.def.
 - [ ] Remove unused fields from def.
 - [x] Wrap all public modules in amqp_client_eio module.
 - [-] Add constraints to the channel class
 - [ ] Test error handling, by inserting random errors in the code.
 - [ ] When receving a body, create a buffer to hold all data and read
       into that.
 - [ ] Use flow to control queue lengths for channels.
 - [x] Handle connection block/unblock

## API support
 - [x] Queue create / delete
 - [ ] Channel open / close
 - [x] Exchange operations
 - [ ] RPC endpoint (client / server)
 - [x] Connection open / close
