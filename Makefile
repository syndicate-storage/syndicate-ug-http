.PHONY: all dep run clean

DEPENDENCIES = `cat DEPENDENCIES`

all: run

dep:
	npm install $(DEPENDENCIES)

run: dep
	node syndicate-ug-http.js
	
clean:
	
