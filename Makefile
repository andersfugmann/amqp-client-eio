.PHONY:build
build:
	dune build

.PHONY:test
test:
	OCAMLRUNPARM=b dune exec bin/main.exe
	@echo "ok"

.PHONY: integration
integration:
	dune build @integration --release
	@echo "ok"

.PHONY: clean
clean:
	dune clean


.PHONY: doc
doc:
	dune build @doc

.PHONY: update-spec
update-spec:
	@echo "Retrieving AMQP spec from RabbitMQ servers"
	curl --fail https://www.rabbitmq.com/resources/specs/amqp0-9-1.extended.xml > spec/amqp0-9-1.extended.xml

.PHONY: gp-pages
gh-pages: doc
	git clone `git config --get remote.origin.url` .gh-pages --reference .
	git -C .gh-pages checkout --orphan gh-pages
	git -C .gh-pages reset
	git -C .gh-pages clean -dxf
	cp  -r _build/default/_doc/_html/* .gh-pages
	git -C .gh-pages add .
	git -C .gh-pages config user.email 'docs@amqp-client'
	git -C .gh-pages commit -m "Update documentation"
	git -C .gh-pages push origin gh-pages -f
	rm -rf .gh-pages
