.SUFFIXES: .erl .beam
.erl.beam:
	erlc +native -W $<

MODS = $(wildcard *.erl)

all: compile

compile: $(patsubst %.erl,%.beam,$(MODS))

run: compile
	./erligig

clean:
	rm -rf *.beam