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
var fs = require('fs');
var async = require('async');
var exec = require('child_process').exec;

var SYNDICATE_CONF_ROOT = __dirname + "/mounts";

function set_syndicate_conf_root(path) {
    SYNDICATE_CONF_ROOT = path;
}

function make_syndicate_conf_path(mount_id) {
    var path = util.format("%s/%s", SYNDICATE_CONF_ROOT, mount_id);
    return utils.get_absolute_path(path);
}

function syndicate_remove_setup(conf_dir, callback) {
    utils.log_info(util.format("removing a syndicate conf %s", conf_dir));
    // check
    exist = utils.check_existance_sync(conf_dir);
    if (exist) {
        // empty dir
        utils.remove_dir_recursively(conf_dir, function(err_remove, result_remove) {
            if(err_remove) {
                callback(err_remove, null);
                return;
            }

            callback(null, true);
            return;
        });
    } else {
        callback(null, true);
        return;
    }
}

function syndicate_check_setup(conf_dir, callback) {
    utils.log_info(util.format("checking up a syndicate conf %s", conf_dir));
    // check
    async.waterfall([
        function(cb) {
            var conf_file = util.format("%s/syndicate.conf", conf_dir);
            utils.check_existance(conf_file, function(err, exist_file) {
                if(err) {
                    cb(err, null);
                    return;
                }

                if(!exist_file) {
                    // empty dir
                    utils.remove_dir_recursively(conf_dir, function(err_remove, result_remove) {
                        if(err_remove) {
                            cb(err_remove, null);
                            return;
                        }

                        cb(null, false);
                        return;
                    });
                    return;
                } else {
                    cb(null, true);
                    return;
                }
            });
        }
    ], function(err, exist) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, true);
        return;
    });
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
    utils.log_info(util.format("setting up a syndicate user %s", user));
    // remove if already exists
    utils.remove_dir_recursively(conf_dir, function(err, data) {
        if(err) {
            callback(err, null);
            return;
        }

        var conf_file = util.format("%s/syndicate.conf", conf_dir);
        var cmd = util.format("syndicate -d --trust_public_key -c %s setup %s %s %s", conf_file, user, cert_path, ms_url);
        utils.log_debug(cmd);
        var child = exec(cmd, function(error, stdout, stderr) {
            if (error) {
                // in this case, it may leave dirties
                utils.remove_dir_recursively(conf_dir, function(err, data) {
                    if(err) {
                        callback(err, null);
                        return;
                    }

                    callback(error, null);
                    return;
                });
                return;
            }

            callback(null, stdout);
        });
    });
}

function syndicate_import_gateway(conf_dir, cert_path, callback) {
    /*
        syndicate -d \
                -c ~/.syndicate_http/suser1@syndicate.org/syndicate.conf \
                import_gateway \
                suser1\@syndicate.org
    */
    utils.log_info("importing a syndicate gateway");

    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d -c %s import_gateway %s force", conf_file, cert_path);
    utils.log_debug(cmd);
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
    utils.log_info(util.format("reloading a syndicate user %s", user));

    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d -c %s reload_user_cert %s", conf_file, user);
    utils.log_debug(cmd);
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
    utils.log_info(util.format("reloading a syndicate volume %s", volume));

    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d -c %s reload_volume_cert %s", conf_file, volume);
    utils.log_debug(cmd);
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
    utils.log_info(util.format("reloading a syndicate gateway %s", gateway));

    var conf_file = util.format("%s/syndicate.conf", conf_dir);
    var cmd = util.format("syndicate -d -c %s reload_gateway_cert %s", conf_file, gateway);
    utils.log_debug(cmd);
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
    check_user: function(mount_id, callback) {
        var user_syndicate_dir = make_syndicate_conf_path(mount_id);
        syndicate_check_setup(user_syndicate_dir, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, data);
        });
    },
    setup_user: function(mount_id, ms_url, user, cert_path, callback) {
        var user_syndicate_dir = make_syndicate_conf_path(mount_id);
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

            callback(null, util.format("setup an user - MountID(%s) / U(%s)", mount_id, user));
        });
    },
    remove_user: function(mount_id, callback) {
        var user_syndicate_dir = make_syndicate_conf_path(mount_id);
        syndicate_remove_setup(user_syndicate_dir, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, data);
        });
    },
    setup_gateway: function(mount_id, volume, gateway, anonymous, cert_path, callback) {
        var user_syndicate_dir = make_syndicate_conf_path(mount_id);

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

                callback(null, util.format("setup a gateway - MountID(%s) / G(%s) / A(%s)", mount_id, gateway, anonymous));
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

                callback(null, util.format("setup a gateway - MountID(%s) / G(%s) / A(%s)", mount_id, gateway, anonymous));
            });
        }
    },
    set_syndicate_conf_root: set_syndicate_conf_root,
    get_configuration_path: make_syndicate_conf_path
};
