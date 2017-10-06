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
var syndicate = require('syndicate-storage');
var nodeCache = require('node-cache');

function safe_close_fh(ug, fh) {
    try {
        if(ug) {
            utils.log_debug("safe_close_fh: calling syndicate.close");
            syndicate.close(ug, fh);
        }
    } catch (ex) {
        utils.log_error(util.format("safe_close_fh: exception occured: %s", ex));
    }
}

function safe_close_gateway(ug) {
    try {
        if(ug) {
            // shutdown UG
            utils.log_debug("safe_close_gateway: calling syndicate.shutdown");
            syndicate.shutdown(ug);
        }
    } catch (ex) {
        utils.log_error(util.format("safe_close_gateway: exception occured: %s", ex));
    }
}

function close_all_fds(ug, fd_map, callback) {
    var keys = fd_map.keys();
    var i;
    for(i=0;i<keys.length;i++) {
        var key = keys[i];
        var stat = fd_map.get(key);
        if(stat) {
            safe_close_fh(ug, stat.fh);
        }

        fd_map.del(key);
    }
    callback(null, "succeed");
}

function close_gateway(ug, callback) {
    // close ug
    safe_close_gateway(ug);
    callback(null, "succeed");
    return;
}

function get_next_fd(fd_table) {
    fd_table.last_fd++;
    var fd = fd_table.last_fd;
    return fd;
}

function add_fd(fd_table, path, fh, flag, callback) {
    var fd = get_next_fd(fd_table);
    utils.log_debug(util.format("add_fd: adding a new file handle - %d", fd));

    fd_table.fd_map.set(fd, {
        'fd': fd,
        'fh': fh,
        'path': path,
        'flag': flag
    });

    callback(null, fd);
    return;
}

function remove_fd(fd_table, fd, callback) {
    utils.log_debug(util.format("remove_fd: closing a file handle - %s", fd));
    fd_table.fd_map.del(fd);

    callback(null, fd);
    return;
}

function get_fd(fd_table, fd, callback) {
    utils.log_debug(util.format("get_fd: retriving a file handle - %s", fd));

    var stat = fd_table.fd_map.get(fd);
    if(stat) {
        // extend cache's ttl
        fd_table.fd_map.ttl(fd);

        callback(null, stat);
        return;
    }

    callback(util.format("get_fd: could not find a file handle - %s", fd), null);
    return;
}

function reset_fd_ttl(fd_table, fd, callback){
    utils.log_debug(util.format("reset_fd_ttl: resetting a file handle TTL - %s", fd));

    var stat = fd_table.fd_map.get(fd);
    if(stat) {
        // reset cache's ttl
        fd_table.fd_map.ttl(fd);

        callback(null, "succeed");
        return;
    }

    callback(util.format("reset_fd_ttl: could not find a file handle - %s", fd), null);
    return;
}

/**
 * Expose root class
 */
module.exports = {
    init: function(user, volume, gateway, anonymous, debug_level, config_root_path, callback) {
        var config_path = util.format("%s/syndicate.conf", config_root_path);

        try {
            var euser = user;
            if(anonymous) {
                euser = "ANONYMOUS";
            }

            var opts = syndicate.create_opts(euser, volume, gateway, debug_level, config_path);

            // init UG
            utils.log_debug("init: calling syndicate.init");
            var ug = syndicate.init(opts);

            // file descriptors
            var fd_map = new nodeCache({
                stdTTL: 3600,
                checkperiod: 600,
                useClones: false
            });

            fd_map.on("expired", function(key, fh) {
                utils.log_debug(util.format("closing an expired file handle - %s", key));
                safe_close_fh(ug, fh);
            });

            var gateway_info = {
                user: user,
                anonymous: anonymous,
                volume: volume,
                gateway: gateway,
                config_path: config_path,
                debug_level: debug_level
            };

            var fd_table = {
                fd_map: fd_map,
                last_fd: 0
            };

            var gateway_state = {
                ug: ug,
                info: gateway_info,
                fd_table: fd_table,
                close: function(callback) {
                    // close all files opened
                    close_all_fds(ug, fd_map, function(err, data) {
                        if(err) {
                            callback(err, null);
                            return;
                        }

                        close_gateway(ug, callback);
                        return;
                    });
                },
                open_fd: function(path, fh, flag, callback) {
                    add_fd(fd_table, path, fh, flag, callback);
                },
                close_fd: function(fd, callback) {
                    remove_fd(fd_table, fd, callback);
                },
                stat_fd: function(fd, callback) {
                    get_fd(fd_table, fd, callback);
                },
                extend_fd_ttl: function(fd, callback) {
                    reset_fd_ttl(fd_table, fd, callback);
                }
            };
            callback(null, gateway_state);
            return;
        } catch (ex) {
            callback(util.format("init: exception occured: %s", ex), null);
            return;
        }
    },
    close: function(gateway_state, callback) {
        // close all files opened
        close_all_fds(gateway_state.ug, gateway_state.fd_table.fd_map, function(err, data) {
            if(err) {
                callback(err, null);
                return;
            }

            close_gateway(gateway_state.ug, callback);
            return;
        });
    },
    open_fd: function(gateway_state, path, fh, flag, callback) {
        add_fd(gateway_state.fd_table, path, fh, flag, callback);
    },
    close_fd: function(gateway_state, fd, callback) {
        remove_fd(gateway_state.fd_table, fd, callback);
    },
    stat_fd: function(gateway_state, fd, callback) {
        get_fd(gateway_state.fd_table, fd, callback);
    },
    extend_fd_ttl: function(gateway_state, fd, callback) {
        reset_fd_ttl(gateway_state.fd_table, fd, callback);
    }
};
