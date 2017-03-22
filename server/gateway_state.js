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
var syndicate = require('syndicate-drive');
var nodeCache = require('node-cache');

function safe_close_fh(ug, fh) {
    if(ug) {
        utils.log_debug("SAFECLOSE: calling syndicate.close");
        syndicate.close(ug, fh);
    }
}

function safe_close_gateway(ug) {
    if(ug) {
        // shutdown UG
        utils.log_debug("SHUTDOWN: calling syndicate.shutdown");
        syndicate.shutdown(ug);
    }
}

/**
 * Expose root class
 */
module.exports = {
    // create a new gateway state
    create: function(user, volume, gateway, config_path) {
        utils.log_debug("INIT: calling syndicate.create_opts");
        var conf_file = util.format("%s/syndicate.conf", config_path);
        var opts = syndicate.create_opts(user, volume, gateway, 0, conf_file);
        
        // init UG
        utils.log_debug("INIT: calling syndicate.init");
        var ug = syndicate.init(opts);
        
        // setup state
        var fd_map = new nodeCache({
            stdTTL: 3600,
            checkperiod: 600,
            useClones: false
        });

        fd_map.on("expired", function(key, fh) {
            utils.log_debug(util.format("closing an expired file handle - %s", key));
            safe_close_fh(ug, fh);
        });
        
        var gateway_state = {
            user: user,
            volume: volume,
            gateway: gateway,
            config_path: conf_file,
            opts: opts,
            ug: ug,
            fd_map: fd_map,
            last_fd: 0,
            io_statistics: {},
        };
        
        return gateway_state;
    },
    // destroy a gateway state
    destroy: function(gateway_state) {
        // destroy
        // close all files opened
        var key;
        for(key in gateway_state.fd_map) {
            utils.log_debug(util.format("closing a missing file handle - %s", key));
            var stat = gateway_state.fd_map.get(key);
            if(stat) {
                safe_close_fh(gateway_state.ug, stat.fh);
                utils.log_debug(util.format("file handle closed - path(%s), flag(%s)", stat.path, stat.flag));
            }
            
            gateway_state.fd_map.del(key);
        }
        
        // close ug
        safe_close_gateway(gateway_state.ug);
    },
    /*
    statistics_keys: {
        RESPONSE: "response",
        RESPONSE_DATA: "response_data",
        RESPONSE_ERROR: "response_error",
        REQUEST: "request",
        REQUEST_GET: "request_get",
        REQUEST_POST: "request_post",
        REQUEST_DELETE: "request_delete",
        FILE_OPENED: "file_opened",
        FILE_READ: "file_read",
        FILE_WRITE: "file_write",
    },
    // statistics
    get_statistics: function(gateway_state, key) {
        var val = null;
        if(key in gateway_state.io_statistics) {
            val = gateway_state.io_statistics[key];
        } else {
            val = 0;
        }
        return val;
    },
    inc_statistics: function(gateway_state, key) {
        var val = null;
        if(key in gateway_state.io_statistics) {
            val = gateway_state.io_statistics[key];
        } else {
            val = 0;
        }

        gateway_state.io_statistics[key] = val+1;
        utils.log_debug("inc_statistics " + key + " - " + gateway_state.io_statistics[key]);
    },
    dec_statistics: function(gateway_state, key) {
        var val = null;
        if(key in gateway_state.io_statistics) {
            val = gateway_state.io_statistics[key];
        } else {
            val = 0;
        }

        if(val-1 >= 0) {
            gateway_state.io_statistics[key] = val - 1;
        }

        utils.log_debug("stat_dec " + key + " - " + gateway_state.io_statistics[key]);
    },
    */
    create_file_handle: function(gateway_state, path, fh, flag) {
        gateway_state.last_fd++;
        var fd = gateway_state.last_fd;
        utils.log_debug(util.format("generate a new file handle - %d", fd));

        gateway_state.fd_map.set(fd, {
            'fd': fd,
            'fh': fh,
            'path': path,
            'flag': flag
        });
        
        return fd;
    },
    destroy_file_handle: function(gateway_state, fd) {
        utils.log_debug(util.format("closing a file handle - %s", fd));

        var stat = gateway_state.fd_map.get(fd);
        if(stat) {
            utils.log_debug(util.format("file handle closed - fh(%d), path(%s), flag(%s)", stat.fh, stat.path, stat.flag));
        }
        
        gateway_state.fd_map.del(fd);
    },
    stat_file_handle: function(gateway_state, fd) {
        var stat = gateway_state.fd_map.get(fd);
        
        if(stat !== undefined) {
            // extend cache's ttl
            gateway_state.fd_map.ttl(fd);
            return stat;
        } else {
            return null;
        }
    },
    extend_file_handle_ttl: function(gateway_state, fd) {
        var stat = gateway_state.fd_map.get(fd);

        if(stat !== undefined) {
            // extend cache's ttl
            gateway_state.fd_map.ttl(fd);
            return true;
        }

        return false;
    }
};
