build:
	dune build

.PHONY:test
test:
	OCAMLRUNPARM=b dune exec bin/main.exe


.PHONY: clean
clean:
	dune clean
