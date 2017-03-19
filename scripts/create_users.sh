#! /bin/bash

# create a user
syndicate create_user "suser_rest@syndicate.org" auto max_volumes=-1 max_gateways=-1

# export user
syndicate export_user "suser_rest@syndicate.org" ~/.
