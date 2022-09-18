(lang dune 3.4)

(name amqp_client_eio)
(generate_opam_files true)

(source
 (github andersfugmann/amqp-client-eio))

(authors "Anders Fugmann")

(maintainers "Anders Fugmann")

(license BSD-3-Clause)

(documentation https://andersfugmann.github.io/amqp-client-eio/index.html)

(package
 (name amqp_client_eio)
 (synopsis "AMQP client based on eio (Multicore / effects")
 (description "AMQP client based on eio (Multicore / effects")
 (depends (ocaml (>= :"5.0.0")) eio eio_main eio_linux ezxmlm)
 (tags
  (topics "AMQP eio multicore library ocaml" )))

; See the complete stanza docs at https://dune.readthedocs.io/en/stable/dune-files.html#dune-project
