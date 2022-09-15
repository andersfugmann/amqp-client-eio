Define one exception type with a polymorphic variant for all errors
 - [x] Add an apply with named arguments function to genspec.ml (reverse of init)
 - [x] Hide all defs and construct {client,server}_{request,reply} function to guarantee correct usage.
 - [x] Create a generic framework for communicating (used by both the connection and channel modules)
 - [x] Add type constraints to Protocol.Spec.def and
       Protocol.Content.def. Identify 't'!
 - [ ] Remove unused fields from def.
 - [ ] Wrap all public modules in amqp_client_eio module.
       Win: All modules still see everything. Outside libraries cannot
       see anything (Which is actually not completly correct. Outside
       modules can see amq_client_eio__queue et al. We might want to
       rethink that.
 - [ ] Add constraints to the channel class
 - [ ] Test error handling, by inserting random errors in the code.
 - [ ] When receving a body, create a buffer to hold all data and read
       into that.
 - [ ] Block channel if the channel queue is full, and unblock when it
       becomes available.
