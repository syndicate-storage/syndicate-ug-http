.PHONY: all clean

all: install

install:
	npm install -g ./cli/
	npm install ./server

clean:
	
