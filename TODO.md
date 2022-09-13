Define one exception type with a polymorphic variant for all errors
 - [ ] Wrap all public modules in amqp_client_eio module
 - [ ] Create signatures for all public modules in amqp_client_eio
 - [ ] Add constraints to the channel class
 - [x] Add an apply with named arguments function to genspec.ml (reverse of init)
 - [x] Hide all defs and construct {client,server}_{request,reply} function to guarantee correct usage.
 - [x] Create a generic framework for communicating (used by both the connection and channel modules)
 - [ ] Test error handling, by inserting random errors in the code.
 - [x] Add type constraints to Protocol.Spec.def and Protocol.Content.def. Identify 't'!

 - [ ] When receving a body, create a buffer to hold all data and read
       into that
 - [ ] Block channel if the channel queue is full, and unblock when it
       becomes available
