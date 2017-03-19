.PHONY: all dep run clean

DEPENDENCIES = `cat DEPENDENCIES`

all: run

dep:
	npm install $(DEPENDENCIES)

install:
	npm install .

run: install
	node syndicate-ug-http.js
	
clean:
	
