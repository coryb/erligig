.SUFFIXES: .erl .beam
.erl.beam:
	erlc +native -W $<

MODS = $(wildcard *.erl)
BEAMS = $(patsubst %.erl,%.beam,$(MODS))

all: compile

compile: $(BEAMS)

$(BEAMS): $(wildcard *.hrl)

run: compile
	./erligig

clean:
	rm -rf *.beam