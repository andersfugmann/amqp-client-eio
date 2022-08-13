Define one exception type with a polymorphic variant for all errors
- [ ] Wrap all public modules in amqp_client_eio module
- [ ] Create signatures for all public modules in amqp_client_eio
- [ ] Add constraints to the channel class
- [ ] Add an apply with named arguments function to genspec.ml (reverse of init)
- [ ] Hide all defs and construct {client,server}_{request,reply} function to guarantee correct usage.
- [ ] Create a generic framework for communicating (used by both the connection and channel modules)
- [ ] Test error handling, by inserting random errors in the code.
- [ ] Add type constraints to Protocol.Spec.def and Protocol.Content.def. Identify 't'!
