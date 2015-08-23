CC=ml-build
all:
	echo "CM.make \"disque.cm\";" | sml
test:
	sml # $(CC) repl.sml
init:
	git submodule update --init --recursive
clean:
	rm -rf *~ .cm cmlib/.cm parcom/.cm
