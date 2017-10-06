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
var restUtils = require('./rest_utils.js');
var utils = require('./utils.js');
var sessionTable = require('./session_table.js');
var mountTable = require('./mount_table.js');
var syndicate = require('syndicate-storage');


function sessions_list(session_table, callback) {
    utils.log_debug(util.format("sessions_list: calling session_table.list"));
    try {
        session_table.list(function(err, session_list) {
            if(err) {
                callback(err, null);
                return;
            }

            var entries = [];
            session_list.forEach(function(session_record) {
                var entry = {
                    name: session_record.name,
                    user: session_record.user,
                    volume: session_record.volume
                };
                entries.push(entry);
            })

            var json_obj = {
                sessions: entries
            };

            callback(null, json_obj);
            return;
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

function sessions_statvfs(session_table, session_name, callback) {
    utils.log_debug(util.format("sessions_statvfs: calling session_table.get_state - %s", session_name));
    try {
        session_table.get_state(session_name, function(err, state) {
            if(err) {
                callback(err, null);
                return;
            }

            if(!state) {
                // state not exist
                callback(util.format("cannot retrieve a session - %s", session_name), null);
                return;
            }

            var ug = state.ug;
            utils.log_debug("sessions_statvfs: calling syndicate.statvfs");
            syndicate.statvfs_async(ug, function(err_statvfs, statvfs) {
                if(err_statvfs) {
                    callback(err_statvfs, null);
                    return;
                }

                callback(null, statvfs);
                return;
            });
        });
    } catch (ex) {
        callback(ex, null);
        return;
    }
}

/**
 * Expose root class
 */
module.exports = {
    get_handler: function(req, res) {
        var options = req.query;
        var path = req.target;
        var session_table = req.session_table;

        var return_rest_data = function(err, data) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            restUtils.return_data(req, res, data);
            return;
        };

        if(options.list !== undefined) {
            // list: ?list
            sessions_list(session_table, return_rest_data);
        } else if(options.statvfs !== undefined) {
            // statvfs: ?statvfs&session=xxx
            var session_name = options.session;
            sessions_statvfs(session_table, session_name, return_rest_data);
        } else {
            res.status(403).send();
        }
    }
};
