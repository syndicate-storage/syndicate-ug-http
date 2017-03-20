#! /bin/bash

# create a user
syndicate create_user "suser_rest@syndicate.org" auto max_volumes=-1 max_gateways=-1

# export a user
syndicate export_user "suser_rest@syndicate.org" ~/.



# register user
node ./setup_user.js -u "suser_rest@syndicate.org" -k suser_rest@syndicate.org
