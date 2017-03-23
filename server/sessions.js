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
var gatewayState = require('./gateway_state.js');
var nodeCache = require('node-cache');
var nodeUUID = require('node-uuid');
var crypto = require('crypto');

function generate_new_session_key() {
    var uuid = nodeUUID.v1();
    var shasum = crypto.createHash('sha1');
    shasum.update(uuid);
    var session_key = shasum.digest('hex');
    return session_key;
}

/**
 * Expose root class
 */
module.exports = {
    init: function() {
        var sessions = new nodeCache({
            stdTTL: 0,
            checkperiod: 0,
            useClones: false
        });
        
        sessions.on("expired", function(name, session) {
            utils.log_debug(util.format("cleaning expired session - %s", name));
        });
        
        return sessions;
    },
    create_session_key: function() {
        return generate_new_session_key();
    },
    create_session: function(sessions, session_name, session_key, ms_url, user, volume, gateway, anonymous, config_path) {
        if(sessions.get(session_name) !== undefined) {
            throw new Error(util.format("session %s already exists", session_name));
        }

        var auser = user;
        if(anonymous) {
            auser = "ANONYMOUS";
        }

        utils.log_debug(util.format("creating a session - MS(%s) / U(%s) / V(%s) / G(%s) / A(%s)", ms_url, user, volume, gateway, anonymous));
        sessions.set(session_name, {
            name: session_name,
            key: session_key,
            ms_url: ms_url,
            user: user,
            volume: volume,
            gateway: gateway,
            anonymous: anonymous,
            gateway_state: gatewayState.create(auser, volume, gateway, config_path),
        });
    },
    destroy_session: function(sessions, session_name) {
        var session = sessions.get(session_name);
        if(session) {
            utils.log_debug(util.format("destroying a session - MS(%s) / U(%s) / V(%s) / G(%s) / A(%s)", session.ms_url, session.user, session.volume, session.gateway, session.anonymous));
            gatewayState.destroy(session.gateway_state);
            sessions.del(session_name);
        }
    },
    list_sessions: function(sessions) {
        var result = [];
        var keys = sessions.keys();
        var i;
        for(i=0;i<keys.length;i++) {
            var key = keys[i];
            var session = sessions.get(key);
            if(session) {
                var obj = {
                    name: session.name,
                    user: session.user
                };
                result.push(obj);
            }
        }
        return result;
    },
    list_sessions_async: function(sessions, callback) {
        var result = [];
        var keys = sessions.keys();
        var i;
        for(i=0;i<keys.length;i++) {
            var key = keys[i];
            var session = sessions.get(key);
            if(session) {
                var obj = {
                    name: session.name,
                    user: session.user
                };
                result.push(obj);
            }
        }
        callback(null, result);
    },
    get_session: function(sessions, session_name) {
        var session = sessions.get(session_name);
        if(session === undefined) {
            return null;
        }
        return session;
    },
    authenticate_async: function(sessions, session_name, session_key, callback) {
        var session = sessions.get(session_name);
        if(session === undefined) {
            callback(null, false, {
                message: util.format("Cannot find a session - %s", session_name)
            });
        } else {
            // verify key
            if(session.key === session_key) {
                callback(null, session);
            } else {
                callback(null, false, {
                    message: util.format("Wrong session_key for a session - %s", session_name)
                });
            }
        }
    },
    serialize_session: function(session) {
        return session.name;
    },
    deserialize_session: function(session_name) {
        return {name: session_name};
    },
};
