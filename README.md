# Syndicate HTTP User Gateway

This module makes Syndicate UG accessible via HTTP/REST.

Run a Server
------------
```
node server/index.js
```


Register an user
----------------
```
node ./setup_user.js -u "suser1@syndicate.org" -k suser1@syndicate.org
```

Options
- u : syndicate user_id
- k : user cert file

Register an anonymous gateway
-----------------------------

```
node ./setup_gateway.js -u suser1@syndicate.org -v pov -a true --anonymous_gateway ug_pov_anonymous --session_name suser1_pov --session_key suser1_pov_secret
```

hadoop dfs -D hadoop.security.credential.provider.path=jceks://hdfs/user/iychoi/.syndicate/hsyndicate.jceks -ls hsyn:///


Options
- u : syndicate user_id
- v : volume
- a : anonymous
-- anonymous_gateway : anonymous gateway name
-- session_name : session name you want to use (user define)
-- session_key : session key for the session (user define)

Register a gateway
------------------

```
node ./setup_gateway.js -u suser1@syndicate.org -v rest_test_volume --gateway_conf ./gateway_config.json --session_name suser1_priv --session_key suser1_priv_secret
```

Options
- u : syndicate user_id
- v : volume
- gateway_conf : gateway configuration file
-- anonymous_gateway : anonymous gateway name
-- session_name : session name you want to use (user define)
-- session_key : session key for the session (user define)
