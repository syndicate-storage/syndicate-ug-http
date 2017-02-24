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

Run Clients
-----------

Use command-line programs for control the server.
- `setup_user.js`: Configure syndicate user
- `setup_gateway.js`: Configure syndicate gateway

The command-line programs controls all REST servers at local cluster. REST servers are described in `config.json` file.
