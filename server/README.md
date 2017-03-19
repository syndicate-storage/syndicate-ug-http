# Syndicate HTTP User Gateway

This module makes Syndicate UG accessible via HTTP/REST.

Prerequisite
------------

This package requires [Syndicate core package](https://github.com/syndicate-storage/syndicate-core) and [Syndicate node module](https://github.com/syndicate-storage/syndicate-node) installed.

Run REST Server
---------------

Execute `syndicate-ug-http.js` file. By default, the module uses a port `8888` by default. You are able to give a specific port number the module will use via `-p` or `--port` parameters. 

Example:

```
node syndicate-ug-http.js -p 18888
```
