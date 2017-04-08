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
var prompt = require('prompt');
var username = require('username');
var exec = require('child_process').exec;
var execSync = require('child_process').execSync;
var path = require('path');

function get_default_client_config_path() {
    return util.format("%s/%s", __dirname, "client_config.json");
}

function parse_args(args) {
    var options = {
        session_name: "",
        session_key: "",
        ms_url: "",
        user: "",
        volume: "",
        anonymous: false,
        anonymous_gateway: "",
        gateway_config_path: "",
        config_path: "",
    };

    // skip first two args
    // 1: node
    // 2: *.js script
    var argv = minimist(args.slice(2));

    // parse
    options.session_name = argv.session_name || "",
    options.session_key = argv.session_key || "",
    options.ms_url = argv.m || "";
    options.user = argv.u || "";
    options.volume = argv.v || "";
    options.anonymous = argv.a || false;
    options.anonymous_gateway = argv.anonymous_gateway || "";
    options.gateway_config_path = argv.gateway_conf || "";
    options.config_path = argv.c || get_default_client_config_path();
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
        conf.ms_url && conf.user && conf.volume && conf.service_hosts && conf.service_hosts.length > 0 && conf.service_port > 0) {
        if(conf.anonymous) {
            if(conf.gateways && conf.gateways.length == 1) {
                return true;
            }
        } else {
            if(conf.gateways && conf.gateways.length > 0 && 
                conf.gateway_cert_paths && conf.gateway_cert_paths.length > 0 &&
                conf.gateways.length == conf.gateway_cert_paths.length &&
                conf.service_hosts.length <= conf.gateways.length) {
                return true;
            }
        }
    }
    return false;
}

function assign_gateways(anonymous, hosts, gateways, certs) {
    var len = hosts.length;
    var assignment = [];
    for(var i=0;i<len;i++) {
        var host = hosts[i];
        var gateway;
        var cert;

        if(anonymous) {
            gateway = gateways[0];
            cert = null;
        } else {
            gateway = gateways[i];
            cert = certs[i];
        }
        
        assignment.push({
            "host": host,
            "gateway": gateway,
            "cert_path": cert,
        });

        utils.log_info(util.format("assign %s - gateway (%s) cert (%s)", host, gateway, cert));
    }
    return assignment;
}

function setup_gateway(node_host, node_port, session_name, session_key, ms_url, user, volume, gateway, anonymous, cert_path, callback) {
    // test 
    var url = util.format("http://%s:%d/gateway/setup", node_host, node_port);
    var complete_callback = function(result, response) {
        if(result instanceof Error) {
            utils.log_error(util.format("[%s:%d] %s", node_host, node_port, result));
            callback(result, null);
        } else {
            utils.log_info(util.format("[%s:%d] %s", node_host, node_port, JSON.stringify(result)));
            callback(null, node_host);
        }
    };

    if(anonymous) {
        restler.post(url, {
            multipart: true,
            data: {
                'session_name': session_name,
                'session_key': session_key,
                'ms_url': ms_url,
                'user': user,
                'volume': volume,
                'gateway': gateway,
                'anonymous': 'true'
            }
        }).on('complete', complete_callback);
    } else {
        fs.stat(cert_path, function(err, stat) {
            if(err) {
                utils.log_error("Error occurred - "+ err);
                callback(util.format("Cannot open cert: %s", cert_path), null);
                return;
            }
            
            restler.post(url, {
                multipart: true,
                data: {
                    'session_name': session_name,
                    'session_key': session_key,
                    'ms_url': ms_url,
                    'user': user,
                    'volume': volume,
                    'gateway': gateway,
                    'anonymous': 'false',
                    'cert': restler.file(cert_path, null, stat.size, null, null)
                }
            }).on('complete', complete_callback);
        });
    }
}

function setup_hadoop_credential(hadoop_user, session_name, session_key, callback) {
    /*
        hadoop credential create \
                    fs.hsyndicate.session.session_name.key \
                    -v session_key \
                    -provider jceks://hdfs/user/iychoi/.syndicate/hsyndicate.jceks
    */
    /*
        hadoop dfs -D hadoop.security.credential.provider.path=jceks://hdfs/user/iychoi/.syndicate/hsyndicate.jceks -ls
    */

    utils.log_info(util.format("setting up a hadoop credential for %s", session_name));
    
    var provider_path = util.format("jceks://hdfs/user/%s/.syndicate/hsyndicate.jceks", hadoop_user);
    var name_pattern = util.format("fs.hsyndicate.session.%s.key", session_name);

    var cmd = util.format("hadoop credential create %s -v %s -provider %s", name_pattern, session_key, provider_path);
    utils.log_debug(cmd);
    var child = exec(cmd, function(error, stdout, stderr) {
        if (error) {
            callback(error, null);
            return;
        }
        callback(null, stdout);
    });
}

function is_hadoop_available() {
    var cmd = "hadoop version";
    try {
        var stdout = execSync(cmd);
        return true;
    } catch (e) {
        // exitcode is not 0
        return false;
    }
}

(function main() {
    utils.log_info("Setup a gateway");

    var param = parse_args(process.argv);
    var gateways = [];
    if(param.anonymous) {
        gateways.push(param.anonymous_gateway);
    }

    var client_config = clientConfig.get_config(param.config_path, {
        "session_name": param.session_name,
        "session_key": param.session_key,
        "ms_url": param.ms_url,
        "user": param.user,
        "volume": param.volume,
        "anonymous": param.anonymous,
        "gateways": gateways
    });
    if(client_config == null) {
        utils.log_error("cannot read configuration");
        process.exit(1);
    }

    if(param.gateway_config_path) {
        var gateway_config = clientConfig.get_config_without_default(param.gateway_config_path);
        client_config = clientConfig.overwrite_config(client_config, gateway_config);
    }

    try {
        async.waterfall([
            function(cb) {
                process_prompt(client_config, function(err, configuration) {
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
                var gateway_assignment = assign_gateways(configuration.anonymous, configuration.service_hosts, configuration.gateways, configuration.gateway_cert_paths);
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
                        setup_gateway(assignment.host, configuration.service_port, configuration.session_name, configuration.session_key, 
                            configuration.ms_url, configuration.user, configuration.volume, assignment.gateway, configuration.anonymous, assignment.cert_path, callback);
                    };
                });
                
                //async.series(calls, function(err, results) {
                async.parallel(calls, function(err, results) {
                    if(err) {
                        cb(err, null);
                        return;
                    }

                    utils.log_info(util.format("Setup a gateway of a user (%s) and a volume (%s)", configuration.user, configuration.volume));
                    utils.log_info(util.format("Use a session name (%s) to access", configuration.session_name));

                    if(is_hadoop_available()) {
                        var hadoop_user = username.sync();
                        setup_hadoop_credential(hadoop_user, configuration.session_name, configuration.session_key, function(err, data) {
                            if(err) {
                                utils.log_error(util.format("Could not setup a hadoop credential: %s", err));
                                return;
                            }
                            utils.log_info(util.format("Successfully setup a hadoop credential: %s", configuration.session_name));
                        });
                    } else {
                        utils.log_info("Hadoop is not accessible - ignoring setting up a hadoop credential");
                    }

                    cb(null, results);
                    return;
                });
            }
        ], function(err, data) {
            if(err) {
                utils.log_error(err);
                process.exit(1);
            }
            process.exit(0);
        });
    } catch (e) {
        utils.log_error(util.format("Exception occured: %s", e));
        process.exit(1);
    }
})();
