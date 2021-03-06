#!/usr/bin/env node
/*
   Copyright 2016 The Trustees of University of Arizona

   Licensed under the Apache License, Version 2.0 (the "License" );
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

var util = require('util');
var utils = require('./utils.js');
var clientConfig = require('./client_config.js');
var restler = require('restler');
var minimist = require('minimist');
var fs = require('fs');
var async = require('async');
var path = require('path');

function get_default_client_config_path() {
    return util.format("%s/%s", __dirname, "client_config.json");
}

function parse_args(args) {
    var options = {
        ms_url: "",
        user: "",
        user_cert_path: "",
        config_path: "",
    };

    // skip first two args
    // 1: node
    // 2: *.js script
    var argv = minimist(args.slice(2));

    // parse
    options.ms_url = argv.m || "";
    options.user = argv.u || "";
    options.user_cert_path = argv.k || "";
    options.config_path = argv.c || get_default_client_config_path();

    if(options.user == "") {
        options.user = extract_user_from_cert(options.user_cert_path);
    }
    return options;
}

function extract_user_from_cert(cert_path) {
    if(cert_path.indexOf("@") >= 0) {
        // correct user format
        var idx = cert_path.lastIndexOf("/");
        if(idx >= 0) {
            cert_path = cert_path.substring(idx + 1);
        }

        if(cert_path.endsWith(".pkey")) {
            // if it's a pkey file
            return cert_path.substring(0, cert_path.length - 5);
        } else {
            return cert_path;
        }
    }
    return null;
}

function check_config(conf) {
    if(conf.ms_url && conf.user && conf.user_cert_path && conf.service_hosts && conf.service_hosts.length > 0 && conf.service_port > 0) {
        return true;
    }
    return false;
}

function setup_user(node_host, node_port, ms_url, user, cert_path, callback) {
    // test
    var url = util.format("http://%s:%d/user/setup", node_host, node_port);
    fs.stat(cert_path, function(err, stat) {
        if(err) {
            utils.log_error(util.format("error occurred - %s", err));
            callback(util.format("cannot open cert: %s", cert_path), null);
            return;
        }

        fs.readFile(cert_path, function(err, data) {
            if(err) {
                utils.log_error(util.format("error occurred - %s", err));
                callback(util.format("cannot read cert: %s", cert_path), null);
                return;
            }

            restler.post(url, {
                multipart: false,
                data: {
                    ms_url: ms_url,
                    user: user,
                    cert: data
                }
            }).on('complete', function(result, response) {
                if(result instanceof Error) {
                    utils.log_error(util.format("[%s:%d] %s", node_host, node_port, result));
                    callback(result, null);
                    return;
                } else {
                    utils.log_info(util.format("[%s:%d] %s", node_host, node_port, JSON.stringify(result)));
                    callback(null, node_host);
                    return;
                }
            });
        });
    });
}

(function main() {
    utils.log_info("Setup a user");

    var param = parse_args(process.argv);
    var client_config = clientConfig.get_config(param.config_path, {
        "ms_url": param.ms_url,
        "user": param.user,
        "user_cert_path": param.user_cert_path
    });
    if(client_config == null) {
        utils.log_error("cannot read configuration");
        process.exit(1);
    }

    if(!check_config(client_config)) {
        utils.log_error("arguments are not given properly");
        process.exit(1);
    }

    try {
        var host_list = client_config.service_hosts;
        var service_port = client_config.service_port;
        var calls = {};

        host_list.forEach(function(host) {
            calls[host] = function(callback) {
                setup_user(host, service_port, client_config.ms_url, client_config.user, client_config.user_cert_path, callback);
            };
        });

        async.parallel(calls, function(err, results) {
            if(err === null) {
                utils.log_info(util.format("setup a user - %s", client_config.user));
            }
        });
    } catch (e) {
        utils.log_error(util.format("Exception occured: %s", e));
    }
})();
