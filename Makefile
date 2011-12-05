.SUFFIXES: .erl .beam
.erl.beam:
	erlc -W $<

MODS = $(wildcard *.erl)

all: compile

compile: $(patsubst %.erl,%.beam,$(MODS))

clean:
	rm -rf *.beam