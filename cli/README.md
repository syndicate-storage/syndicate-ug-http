# Syndicate HTTP User Gateway CLI

This module provides command line interface to Syndicate UG HTTP Server.

Run
---

Make sure that Syndicate HTTP Server is running.

Use command-line programs for controlling the server.
- `syndicate-http-setup-gateway`: Configure syndicate user
- `syndicate-http-setup-gateway`: Configure syndicate gateway


Examples
--------

```
node ./setup_user.js -u "suser_rest@syndicate.org" -k suser_rest@syndicate.org
```

```
node ./setup_gateway.js -u suser_rest@syndicate.org -v pov -a true --anonymous_gateway ug_pov_anonymous --session_name suser_rest_pov --session_key suser_rest_pov
```
