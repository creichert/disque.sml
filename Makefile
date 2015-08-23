CC=ml-build
all:
	echo "CM.make \"disque.cm\";" | sml
test:
	sml # $(CC) repl.sml
clean:
	rm -rf *~
