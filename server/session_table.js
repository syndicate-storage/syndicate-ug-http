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
var sqlite3 = require('sqlite3').verbose();
var gatewayState = require('./gateway_state.js');

function make_session_key_hash(session_name, session_key) {
    var seed = util.format("seed%sabddeff123%s", session_name, session_key);
    return utils.generate_checksum(seed)
}

function open_database(path, callback) {
    var db = new sqlite3.Database(path, function(err) {
        if(err) {
            callback(err, null);
            return;
        }
    });

    var sql = "CREATE TABLE IF NOT EXISTS session_table (name TEXT PRIMARY KEY, key TEXT NOT NULL, mount_id TEXT NOT NULL, volume TEXT NOT NULL, gateway TEXT NOT NULL, anonymous INTEGER)";
    db.run(sql, function(err) {
        if(err) {
            callback(err, null);
            return;
        }

        var sql_index = "CREATE INDEX IF NOT EXISTS session_table_mount_id_idx ON session_table (mount_id)"
        db.run(sql_index, function(err_index) {
            if(err_index) {
                callback(err_index, null);
                return;
            }

            callback(null, db);
            return;
        });
    });
}

function close_database(db, callback) {
    if(!db) {
        callback("db is null", null);
        return;
    }

    db.close(function(err) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, "closed");
        return;
    });
}

function get_session(db, session_name, callback) {
    utils.log_debug(util.format("retriving a session - NAME(%s)", session_name));

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "SELECT s.name as name, s.key as key, s.mount_id as mount_id, s.volume as volume, s.gateway as gateway, s.anonymous as anonymous, m.ms_url as ms_url, m.user as user FROM session_table as s, mount_table as m WHERE s.mount_id = m.mount_id and s.name = ?";
    db.get(sql, [session_name], function(err, row) {
        if(err) {
            callback(err, null);
            return;
        }

        if(!row) {
            callback(null, null);
            return;
        }

        var b_anonymous = row.anonymous > 0 ? true : false;

        var session_record = {
            name: row.name,
            key: row.key,
            mount_id: row.mount_id,
            volume: row.volume,
            gateway: row.gateway,
            anonymous: b_anonymous,
            ms_url: row.ms_url,
            user: row.user
        };
        callback(null, session_record);
        return;
    });
}

function check_session(db, session_name, callback) {
    if(!db) {
        callback("db is null", null);
        return;
    }

    get_session(db, session_name, function(err, record) {
        if(err) {
            callback(err, null);
            return;
        }

        if(!record) {
            callback(null, false);
            return;
        }

        callback(null, true);
        return;
    });
}

function list_sessions_by_mount_id(db, mount_id, callback) {
    utils.log_debug(util.format("listing sessions by mount_id - ID(%s)", mount_id));

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "SELECT s.name as name, s.key as key, s.mount_id as mount_id, s.volume as volume, s.gateway as gateway, s.anonymous as anonymous, m.ms_url as ms_url, m.user as user FROM session_table as s, mount_table as m WHERE s.mount_id = m.mount_id and s.mount_id = ?";
    db.all(sql, [mount_id], function(err, rows) {
        if(err) {
            callback(err, null);
            return;
        }

        var session_records = []
        for(var i=0;i<rows.length;i++) {
            var row = rows[i];
            var b_anonymous = row.anonymous > 0 ? true : false;

            var session_record = {
                name: row.name,
                key: row.key,
                mount_id: row.mount_id,
                volume: row.volume,
                gateway: row.gateway,
                anonymous: b_anonymous,
                ms_url: row.ms_url,
                user: row.user
            };
            session_records.push(session_record);
        }

        callback(null, session_records);
        return;
    });
}

function count_sessions_by_mount_id(db, mount_id, callback) {
    utils.log_debug(util.format("counting sessions by mount_id - ID(%s)", mount_id));

    if(!db) {
        callback("db is null", null);
        return;
    }

    list_sessions_by_mount_id(db, mount_id, function(err, records) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, records.length);
        return;
    });
}

function list_sessions(db, callback) {
    utils.log_debug("listing sessions");

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "SELECT s.name as name, s.key as key, s.mount_id as mount_id, s.volume as volume, s.gateway as gateway, s.anonymous as anonymous, m.ms_url as ms_url, m.user as user FROM session_table as s, mount_table as m WHERE s.mount_id = m.mount_id";
    db.all(sql, [], function(err, rows) {
        if(err) {
            callback(err, null);
            return;
        }

        var session_records = []
        for(var i=0;i<rows.length;i++) {
            var row = rows[i];
            var b_anonymous = row.anonymous > 0 ? true : false;

            var session_record = {
                name: row.name,
                key: row.key,
                mount_id: row.mount_id,
                volume: row.volume,
                gateway: row.gateway,
                anonymous: b_anonymous,
                ms_url: row.ms_url,
                user: row.user
            };
            session_records.push(session_record);
        }

        callback(null, session_records);
        return;
    });
}

function add_session(db, session_name, session_key, mount_id, volume, gateway, anonymous, callback) {
    utils.log_debug(util.format("adding a session - NAME(%s) / ID(%s) / V(%s) / G(%s) / A(%s)", session_name, mount_id, volume, gateway, anonymous));

    if(!db) {
        callback("db is null", null);
        return;
    }

    var session_key_hash = make_session_key_hash(session_name, session_key);

    var sql = "INSERT INTO session_table (name, key, mount_id, volume, gateway, anonymous) values (?, ?, ?, ?, ?, ?)";
    db.run(sql, [session_name, session_key_hash, mount_id, volume, gateway, anonymous], function(err) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, "succeed");
        return;
    });
}

function remove_session(db, session_name, callback) {
    utils.log_debug(util.format("removing a session - NAME(%s)", session_name));

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "DELETE FROM session_table WHERE name = ?";
    db.run(sql, [session_name], function(err) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, "succeed");
        return;
    });
}

function remove_sessions_by_mount_id(db, mount_id, callback) {
    utils.log_debug(util.format("removing a session by mount_id - ID(%s)", mount_id));

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "DELETE FROM session_table WHERE mount_id = ?";
    db.run(sql, [mount_id], function(err) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, "succeed");
        return;
    });
}

function check_session_key(db, session_name, session_key, callback) {
    utils.log_debug(util.format("checking a session key - NAME(%s)", session_name));

    if(!db) {
        callback("db is null", null);
        return;
    }

    get_session(db, session_name, function(err, record) {
        if(err) {
            callback(err, null);
            return;
        }

        if(!record) {
            callback(util.format("Session does not exist - %s", session_name), null);
            return;
        }

        if(make_session_key_hash(record.name, session_key) == record.key) {
            callback(null, true);
            return;
        } else {
            callback(util.format("Session does not exist - %s", session_name), null);
            return;
        }
    });
}

function authenticate_session(db, session_name, session_key, callback) {
    utils.log_debug(util.format("authenticating a session - NAME(%s)", session_name));

    if(!db) {
        callback(null, false, {
            message: "db is null"
        });
        return;
    }

    get_session(db, session_name, function(err, record) {
        if(err) {
            callback(null, false, {
                message: err
            });
            return;
        }

        if(!record) {
            callback(null, false, {
                message: util.format("Session does not exist - %s", session_name)
            });
            return;
        }

        if(record.anonymous) {
            callback(null, record);
            return;
        }

        if(make_session_key_hash(record.name, session_key) == record.key) {
            callback(null, record);
            return;
        } else {
            callback(null, false, {
                message: util.format("Session does not exist - %s", session_name)
            });
            return;
        }
    });
}

function serialize_session(db, session, callback) {
    callback(null, session.name);
}

function deserialize_session(db, session_name, callback) {
    if(!db) {
        callback("db is null", null);
        return;
    }

    get_session(db, session_name, function(err, record) {
        if(err) {
            callback(err, null);
            return;
        }

        if(!record) {
            callback(util.format("Session does not exist - %s", session_name, null));
            return;
        }

        callback(null, record);
        return;
    });
}

function mount_gateway_state(db, gateway_state_table, session_name, configuration_path, callback) {
    if(!db) {
        callback("db is null", null);
        return;
    }

    get_session(db, session_name, function(err, record) {
        if(err) {
            callback(err, null);
            return;
        }

        if(!record) {
            callback(util.format("Session does not exist - %s", session_name, null));
            return;
        }

        // run
        var debug_level = 3;
        gatewayState.init(record.user, record.volume, record.gateway, record.anonymous, debug_level, configuration_path, function(err_gs, state) {
            if(err_gs) {
                callback(err_gs, null);
                return;
            }

            gateway_state_table[session_name] = state;
            callback(null, true);
            return;
        });
    });
}

function unmount_gateway_state(db, gateway_state_table, session_name, callback) {
    if(!db) {
        callback("db is null", null);
        return;
    }

    if(session_name in gateway_state_table) {
        var state = gateway_state_table[session_name];
        state.close(function(err, record) {
            if(err) {
                callback(err, null);
                return;
            }

            callback(null, true);
        });
    } else {
        callback(null, true);
    }
}

function get_gateway_state(db, gateway_state_table, session_name, callback) {
    if(!db) {
        callback("db is null", null);
        return;
    }

    if(session_name in gateway_state_table) {
        var state = gateway_state_table[session_name];
        callback(null, state);
        return;
    } else {
        callback(util.format("could not found gateway state - %s", session_name), null);
        return;
    }
}

/**
 * Expose root class
 */
module.exports = {
    init: function(db_path, callback) {
        if(!db_path) {
            db_path = util.format("%s/syndicate_rest.db", __dirname);
        }

        open_database(db_path, function(err, db) {
            if(err) {
                callback(err, null);
                return;
            }

            var gateway_state_table = {};
            var session_table = {
                db_path: db_path,
                db: db,
                gateway_state_table: gateway_state_table,
                close: function(callback) {
                    close_database(db, callback);
                },
                check: function(session_name, callback) {
                    check_session(db, session_name, callback);
                },
                check_key: function(session_name, session_key, callback) {
                    check_session_key(db, session_name, session_key, callback);
                },
                get: function(session_name, callback) {
                    get_session(db, session_name, callback);
                },
                list: function(callback) {
                    list_sessions(db, callback);
                },
                list_by_mount_id: function(mount_id, callback) {
                    list_sessions_by_mount_id(db, mount_id, callback);
                },
                count_by_mount_id: function(mount_id, callback) {
                    count_sessions_by_mount_id(db, mount_id, callback);
                },
                add: function(session_name, session_key, mount_id, volume, gateway, anonymous, callback) {
                    add_session(db, session_name, session_key, mount_id, volume, gateway, anonymous, callback);
                },
                remove: function(session_name, callback) {
                    remove_session(db, session_name, callback);
                },
                remove_by_mount_id: function(mount_id, callback) {
                    remove_sessions_by_mount_id(db, mount_id, callback);
                },
                authenticate: function(session_name, session_key, callback) {
                    authenticate_session(db, session_name, session_key, callback);
                },
                serialize: function(session, callback) {
                    serialize_session(db, session, callback);
                },
                deserialize: function(session_name, callback) {
                    deserialize_session(db, session_name, callback);
                },
                mount: function(session_name, configuration_path, callback) {
                    mount_gateway_state(db, gateway_state_table, session_name, configuration_path, callback);
                },
                unmount: function(session_name, callback) {
                    unmount_gateway_state(db, gateway_state_table, session_name, callback);
                },
                get_state: function(session_name, callback) {
                    get_gateway_state(db, gateway_state_table, session_name, callback);
                }
            };
            callback(null, session_table);
            return;
        });
    },
    close: function(session_table, callback) {
        close_database(session_table.db, callback);
    },
    check: function(session_table, session_name, callback) {
        check_session(session_table.db, session_name, callback);
    },
    check_key: function(session_table, session_name, session_key, callback) {
        check_session_key(session_table.db, session_name, session_key, callback);
    },
    get: function(session_table, session_name, callback) {
        get_session(session_table.db, session_name, callback);
    },
    list: function(session_table, callback) {
        list_sessions(session_table.db, callback);
    },
    list_by_mount_id: function(session_table, mount_id, callback) {
        list_sessions_by_mount_id(session_table.db, mount_id, callback);
    },
    count_by_mount_id: function(session_table, mount_id, callback) {
        count_sessions_by_mount_id(session_table.db, mount_id, callback);
    },
    add: function(session_table, session_name, session_key, mount_id, volume, gateway, anonymous, callback) {
        add_session(session_table.db, session_name, session_key, mount_id, volume, gateway, anonymous, callback);
    },
    remove: function(session_table, session_name, callback) {
        remove_session(session_table.db, session_name, callback);
    },
    remove_by_mount_id: function(session_table, mount_id, callback) {
        remove_sessions_by_mount_id(session_table.db, mount_id, callback);
    },
    authenticate: function(session_table, session_name, session_key, callback) {
        authenticate_session(session_table.db, session_name, session_key, callback);
    },
    serialize: function(session_table, session, callback) {
        serialize_session(session_table.db, session, callback);
    },
    deserialize: function(session_table, session_name, callback) {
        deserialize_session(session_table.db, session_name, callback);
    },
    mount: function(session_table, session_name, configuration_path, callback) {
        mount_gateway_state(session_table.db, session_table.gateway_state_table, session_name, configuration_path, callback);
    },
    unmount: function(session_table, session_name, callback) {
        unmount_gateway_state(session_table.db, session_table.gateway_state_table, session_name, callback);
    },
    get_state: function(session_table, session_name, callback) {
        get_gateway_state(session_table.db, session_table.gateway_state_table, session_name, callback);
    }
};
