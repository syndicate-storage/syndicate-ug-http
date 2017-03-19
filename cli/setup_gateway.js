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
var utils = require('../lib/utils.js');
var clientConfig = require('../lib/client_config.js');
var restler = require('restler');
var minimist = require('minimist');
var fs = require('fs');
var async = require('async');
var prompt = require('prompt');

function parse_args(args) {
    var options = {
        session_name: "",
        session_key: "",
        ms_url: "",
        user: "",
        volume: "",
        anonymous: "",
        gateway_config_path: "",
        config_path: "",
    };

    // skip first two args
    // 1: node
    // 2: *.js script
    var argv = minimist(args.slice(2));

    // parse
    options.session_name = argv.n || "",
    options.session_key = argv.k || "",
    options.ms_url = argv.m || "";
    options.user = argv.u || "";
    options.volume = argv.v || "";
    options.anonymous = argv.a || false;
    options.gateway_config_path = argv.g || "";
    options.config_path = argv.i || "./client_config.json";
    return options;
}

function process_prompt(conf, callback) {
    prompt.start();
    var prop = [{
        description: "Enter session name",
        type: "string",
        name: "session_name",
        required: true,
        ask: function() {
            if(conf.session_name == undefined || conf.session_name.length == 0) {
                return true;
            }
            return false;
        }
    },
    {
        description: "Enter session key",
        type: "string",
        name: "session_key",
        required: true,
        ask: function() {
            if(conf.session_key == undefined || conf.session_key.length == 0) {
                return true;
            }
            return false;
        }
    }];

    prompt.get(prop, function(err, result) {
        if(err) {
            callback(err, null);
        }

        if(result.session_name) {
            conf.session_name = result.session_name;
        }
        
        if(result.session_key) {
            conf.session_key = result.session_key;
        }

        callback(null, conf);
    });
}

function check_config(conf) {
    if(conf.session_name && conf.session_key && 
        conf.ms_url && conf.user && conf.volume && conf.hosts && conf.hosts.length > 0) {
        if(conf.anonymous) {
            return true;
        } else {
            if(conf.gateways && conf.gateways.length > 0 && 
                conf.gateway_cert_paths && conf.gateway_cert_paths.length > 0 &&
                conf.gateways.length == conf.gateway_cert_paths.length &&
                conf.hosts.length <= conf.gateways.length) {
                return true;
            }
        }
    }
    return false;
}

function assign_gateways(hosts, gateways, certs) {
    var len = hosts.length;
    var assignment = [];
    for(var i=0;i<len;i++) {
        var host = hosts[i];
        var gateway = gateways[i];
        var cert = certs[i];

        assignment.push({
            "host": host,
            "gateway": gateway,
            "cert_path": cert,
        });
    }
    return assignment;
}

function setup_gateway(node_host, session_name, session_key, ms_url, user, volume, gateway, anonymous, cert_path, callback) {
    // test 
    var url = utils.format("http://%s/setup/gateway", node_host);
    var complete_callback = function(result, response) {
        if(result instanceof Error) {
            console.error(util.format("[%s] %s", node_host, result));
            callback(result, null);
        } else {
            console.log(util.format("[%s] %s", node_host, JSON.stringify(result)));
            callback(null, node_host);
        }
    };

    var data_object = {
        'session_name': session_name,
        'session_key': session_key,
        'ms_url': ms_url,
        'user': user,
        'volume': volume,
        'gateway': gateway,
        'anonymous': anonymous
    };

    if(anonymous) {
        restler.post(url, {
            multipart: true,
            data: data_object
        }).on('complete', complete_callback);
    } else {
        fs.stat(cert_path, function(err, stat) {
            if(err) {
                utils.log_error("Error occurred - "+ err);
                callback(util.format("Cannot open cert: %s", cert_path), null);
                return;
            }

            data_object["cert"] = restler.file(cert_path, null, stat.size, null, null);
            
            restler.post(url, {
                multipart: true,
                data: data_object
            }).on('complete', complete_callback);
        });
    }
}

(function main() {
    utils.log_info("Setup a gateway");

    var param = parse_args(process.argv);
    var conf = clientConfig.get_config(param.config_path, {
        "session_name": param.session_name,
        "session_key": param.session_key,
        "ms_url": param.ms_url,
        "user": param.user,
        "volume": param.volume,
        "anonymous": param.anonymous
    });

    if(param.gateway_config_path) {
        var gateway_conf = clientConfig.get_config(param.gateway_config_path);
        conf = clientConfig.overwrite_config(conf, gateway_conf);
    }

    try {
        async.waterfall([
            function(cb) {
                process_prompt(conf, function(err, configuration) {
                    if(err) {
                        cb(new Error("arguments are not given properly"), null);
                        return;
                    }

                    if(!check_config(configuration)) {
                        cb(new Error("arguments are not given properly"), null);
                        return;
                    }

                    cb(null, configuration);
                    return;
                });
            },
            function(configuration, cb) {
                var gateway_assignment = assign_gateways(configuration.hosts, configuration.gateways, configuration.gateway_cert_paths);
                if(gateway_assignment.length <= 0) {
                    cb(new Error("Failed to assign gateways to hosts"), null);
                    return;
                } else {
                    cb(null, {
                        configuration: configuration,
                        gateway_assignment: gateway_assignment
                    });
                    return;
                }
            },
            function(result, cb) {
                // register gateways
                var calls = {};

                var configuration = result.configuration;
                var gateway_assignment = result.gateway_assignment;

                gateway_assignment.forEach(function(assignment) {
                    calls[assignment.host] = function(callback) {
                        setup_gateway(assignment.host, configuration.session_name, configuration.session_key, 
                            configuration.ms_url, configuration.user, configuration.volume, assignment.gateway, conf.anonymous, assignment.cert_path, callback);
                    };
                });
                
                async.parallel(calls, function(err, results) {
                    if(err) {
                        cb(err, null);
                        return;
                    }

                    console.log(utils.format("Setup a gateway of a user (%s) and a volume (%s)", configuration.user, configuration.volume));
                    console.log(utils.format("Use a session name (%s) to access", configuration.session_name));
                    cb(null, results);
                    return;
                });
            }
        ], function(err, data) {
            if(err) {
                console.error(err);
                process.exit(1);
            }
            process.exit(0);
        });
    } catch (e) {
        utils.log_error(utils.format("Exception occured: %s", e));
        process.exit(1);
    }
})();
