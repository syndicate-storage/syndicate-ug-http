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

var utils = require('../lib/utils.js');
var config = require('../lib/config.js');
var restler = require('restler');
var minimist = require('minimist');
var fs = require('fs');
var async = require('async');

function parse_args(args) {
    var options = {
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
    options.ms_url = argv.m || "";
    options.user = argv.u || "";
    options.volume = argv.v || "";
    options.anonymous = argv.a || false;
    options.gateway_config_path = argv.g || "";
    options.config_path = argv.i || "./config.json";
    return options;
}

function check_config(conf) {
    if(conf.ms_url && conf.user && conf.volume && conf.hosts && conf.hosts.length > 0) {
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

function get_session_key(node_host, callback) {
    // test 
    var url = utils.format("http://%s/setup/session", node_host);
    restler.get(url).on('complete', function(result, response) {
        if(result instanceof Error) {
            console.error(util.format("[%s] %s", node_host, result));
            callback(result, null);
        } else {
            var session_key = result.session_key;
            console.log(util.format("[%s] %s", node_host, JSON.stringify(result)));
            callback(null, session_key);
        }
    });
}

function setup_gateway(node_host, session_key, ms_url, user, volume, gateway, anonymous, cert_path, callback) {
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
    var conf = config.get_config(param.config_path, {
        "ms_url": param.ms_url,
        "user": param.user,
        "volume": param.volume,
        "anonymous": param.anonymous
    });

    if(param.gateway_config_path) {
        var gateway_conf = config.get_config(param.gateway_config_path);
        conf = config.overwrite_config(conf, gateway_conf);
    }
    
    if(!check_config(conf)) {
        console.error("arguments are not given properly");
        process.exit(1);
    }

    try {
        var gateway_assignment = assign_gateways(conf.hosts, conf.gateways, conf.gateway_cert_paths);
        if(gateway_assignment.length <= 0) {
            console.error("Failed to assign gateways to hosts");
            process.exit(1);
        }

        async.waterfall([
            function(cb) {
                // create a session key from one of hosts
                get_session_key(gateway_assignment[0].host, function(err, data) {
                    if(err) {
                        console.error("Cannot obtain a new session key");
                        cb(err, null);
                        return;
                    }
                    var session_key = data;
                    console.log(utils.format("Obtained a new session key : %s", session_key));
                    cb(null, session_key);
                });
            },
            function(session_key, cb) {
                // register gateways with the session key obtained
                var calls = {};
                
                gateway_assignment.forEach(function(assignment) {
                    calls[assignment.host] = function(callback) {
                        setup_gateway(assignment.host, session_key, conf.ms_url, conf.user, conf.volume, assignment.gateway, conf.anonymous, assignment.cert_path, callback);
                    };
                });
                
                async.parallel(calls, function(err, results) {
                    if(err) {
                        cb(err, null);
                        return;
                    }

                    console.log(utils.format("Setup a gateway of a user (%s) and a volume (%s)", conf.user, conf.volume));
                    cb(null, results);
                });
            }
        ], function(err, data) {
            if(err) {
                console.error(err);
                return;
            }
        });
    } catch (e) {
        utils.log_error(utils.format("Exception occured: %s", e));
    }
})();
