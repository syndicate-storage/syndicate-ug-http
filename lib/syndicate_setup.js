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

var utils = require('./utils.js');
var fs = require('fs');
var async = require('async');
var exec = require('child_process').exec;
var url = require('url');

function make_syndicate_conf_path(ms_url, user) {
    var ms = url.parse(ms_url);
    var path = util.format("~/.syndicate-ug-http/%s/%s", ms.host, user));
    return utils.resolve_home(path);
}

function syndicate_setup(ms_url, user, conf_dir, cert_path, callback) {
    /*
        syndicate -d --trust_public_key \
                    -c ~/.syndicate_http/suser1@syndicate.org/syndicate.conf \
                    setup \
                    suser1@syndicate.org \
                    suser1\@syndicate.org.pkey \
                    http://demo1.opencloud.cs.arizona.edu:28080
    */
    // remove if already exists
    utils.remove_dir_recursively_sync(conf_dir);

    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d --trust_public_key -c %s setup %s %s %s", conf_file, user, cert_path, ms_url);
    var child = exec(cmd, function(error, stdout, stderr) {
        if (error) {
            // in this case, it may leave dirties
            utils.remove_dir_recursively_sync(conf_dir);
            callback(error, null);
            return;
        }
        callback(null, stdout);
    });
}

function syndicate_import_gateway(conf_dir, cert_path, callback) {
    /*
        syndicate -d \
                -c ~/.syndicate_http/suser1@syndicate.org/syndicate.conf \
                import_gateway \
                suser1\@syndicate.org
    */
    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d -c %s import_gateway %s", conf_file, cert_path);
    var child = exec(cmd, function(error, stdout, stderr) {
        if (error) {
            callback(error, null);
            return;
        }
        callback(null, stdout);
    });
}

function syndicate_reload_user(user, conf_dir, callback) {
    /*
        syndicate -d\
                -c ~/.syndicate_http/suser1@syndicate.org/syndicate.conf \
                reload_user_cert \
                suser1@syndicate.org
    */
    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d -c %s reload_user_cert %s", conf_file, user);
    var child = exec(cmd, function(error, stdout, stderr) {
        if (error) {
            callback(error, null);
            return;
        }
        callback(null, stdout);
    });
}

function syndicate_reload_volume(volume, conf_dir, callback) {
    /*
        syndicate -d\
                -c ~/.syndicate_http/suser1@syndicate.org/syndicate.conf \
                reload_volume_cert \
                volume
    */
    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d -c %s reload_volume_cert %s", conf_file, volume);
    var child = exec(cmd, function(error, stdout, stderr) {
        if (error) {
            callback(error, null);
            return;
        }
        callback(null, stdout);
    });
}

function syndicate_reload_gateway(gateway, conf_dir, callback) {
    /*
        syndicate -d\
                -c ~/.syndicate_http/suser1@syndicate.org/syndicate.conf \
                reload_gateway_cert \
                gateway
    */
    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d -c %s reload_gateway_cert %s", conf_file, gateway);
    var child = exec(cmd, function(error, stdout, stderr) {
        if (error) {
            callback(error, null);
            return;
        }
        callback(null, stdout);
    });
}

/**
 * Expose root class
 */
module.exports = {
    setup_user: function(ms_url, user, cert_path, callback) {
        var user_syndicate_dir = make_syndicate_conf_path(ms_url, user);
        async.waterfall([
            function(cb) {
                syndicate_setup(ms_url, user, user_syndicate_dir, cert_path, function(err, data) {
                    if(err) {
                        cb(err, null);
                        return;
                    }
                    cb(null, data);
                });
            },
            function(result, cb) {
                syndicate_reload_user(user, user_syndicate_dir, function(err, data) {
                    if(err) {
                        cb(err, null);
                        return;
                    }
                    cb(null, data);
                });
            }
        ], function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }
            
            callback(null, util.format("setup an user - %s", user));
        });
    },
    setup_gateway: function(ms_url, user, volume, gateway, anonymous, cert_path, callback) {
        var user_syndicate_dir = make_syndicate_conf_path(ms_url, user);

        if(anonymous) {
            async.waterfall([
                function(cb) {
                    syndicate_reload_volume(volume, user_syndicate_dir, function(err, data) {
                        if(err) {
                            cb(err, null);
                            return;
                        }
                        cb(null, data);
                    });
                },
                function(result, cb) {
                    syndicate_reload_gateway(gateway, user_syndicate_dir, function(err, data) {
                        if(err) {
                            cb(err, null);
                            return;
                        }
                        cb(null, data);
                    });
                }
            ], function(err, data) {
                if(err) {
                    callback(err, null);
                    return;
                }
                
                callback(null, util.format("setup a gateway - %s", gateway));
            });
        } else {
            async.waterfall([
                function(cb) {
                    syndicate_reload_volume(volume, user_syndicate_dir, function(err, data) {
                        if(err) {
                            cb(err, null);
                            return;
                        }
                        cb(null, data);
                    });
                },
                function(result, cb) {
                    syndicate_import_gateway(user_syndicate_dir, cert_path, function(err, data) {
                        if(err) {
                            cb(err, null);
                            return;
                        }
                        cb(null, data);
                    });
                }
            ], function(err, data) {
                if(err) {
                    callback(err, null);
                    return;
                }
                
                callback(null, util.format("setup a gateway - %s", gateway));
            });
        }
    },
    get_configuration_path: function(ms_url, user) {
        return make_syndicate_conf_path(ms_url, user);
    },
};
