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
var url = require('url');
var sqlite3 = require('sqlite3').verbose();


function open_database(path, callback) {
    var db = new sqlite3.Database(path, function(err) {
        if(err) {
            callback(err, null);
            return;
        }
    });

    var sql = "CREATE TABLE IF NOT EXISTS mount_table (mount_id TEXT PRIMARY KEY, ms_url TEXT NOT NULL, user TEXT NOT NULL)";
    db.run(sql, function(err) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, db);
        return;
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

function get_mount(db, mount_id, callback) {
    utils.log_debug(util.format("retriving a mount - ID(%s)", mount_id));

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "SELECT mount_id, ms_url, user FROM mount_table WHERE mount_id = ?";
    db.get(sql, [mount_id], function(err, row) {
        if(err) {
            callback(err, null);
            return;
        }

        if(!row) {
            callback(null, null);
            return;
        }

        var mount_record = {
            mount_id: row.mount_id,
            ms_url: row.ms_url,
            user: row.user
        };
        callback(null, mount_record);
        return;
    });
}

function check_mount(db, mount_id, callback) {
    if(!db) {
        callback("db is null", null);
        return;
    }

    get_mount(db, mount_id, function(err, record) {
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

function list_mounts(db, callback) {
    utils.log_debug("listing mounts");

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "SELECT mount_id, ms_url, user FROM mount_table";
    db.all(sql, [], function(err, rows) {
        if(err) {
            callback(err, null);
            return;
        }

        var mount_records = []
        for(var i=0;i<rows.length;i++) {
            var row = rows[i];
            var mount_record = {
                mount_id: row.mount_id,
                ms_url: row.ms_url,
                user: row.user
            };
            mount_records.push(mount_record);
        }

        callback(null, mount_records);
        return;
    });
}

function add_mount(db, mount_id, ms_url, user, callback) {
    utils.log_debug(util.format("adding a mount - ID(%s) / MS(%s) / U(%s)", mount_id, ms_url, user));

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "INSERT INTO mount_table (mount_id, ms_url, user) values (?, ?, ?)";
    db.run(sql, [mount_id, ms_url, user], function(err) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, "succeed");
        return;
    });
}

function remove_mount(db, mount_id, callback) {
    utils.log_debug(util.format("removing a mount - ID(%s)", mount_id));

    if(!db) {
        callback("db is null", null);
        return;
    }

    var sql = "DELETE FROM mount_table WHERE mount_id = ?";
    db.run(sql, [mount_id], function(err) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, "succeed");
        return;
    });
}

/**
 * Expose root class
 */
module.exports = {
    make_mount_id: function(ms_url, user) {
        var ms = url.parse(ms_url);
        var seed = util.format("seed%s123%s/%s", ms.protocol, ms.host, user);
        return utils.generate_checksum(seed)
    },
    init: function(db_path, callback) {
        if(!db_path) {
            db_path = util.format("%s/syndicate_rest.db", __dirname);
        }

        open_database(db_path, function(err, db) {
            if(err) {
                callback(err, null);
                return;
            }

            mount_table = {
                db_path: db_path,
                db: db,
                close: function(callback) {
                    close_database(db, callback);
                },
                check: function(mount_id, callback) {
                    check_mount(db, mount_id, callback);
                },
                get: function(mount_id, callback) {
                    get_mount(db, mount_id, callback);
                },
                list: function(callback) {
                    list_mounts(db, callback);
                },
                add: function(mount_id, ms_url, user, callback) {
                    add_mount(db, mount_id, ms_url, user, callback);
                },
                remove: function(mount_id, callback) {
                    remove_mount(db, mount_id, callback);
                }
            };
            callback(null, mount_table);
            return;
        });
    },
    close: function(mount_table, callback) {
        close_database(mount_table.db, callback);
    },
    check: function(mount_table, mount_id, callback) {
        check_mount(mount_table.db, mount_id, callback);
    },
    get: function(mount_table, mount_id, callback) {
        get_mount(mount_table.db, mount_id, callback);
    },
    list: function(mount_table, callback) {
        list_mounts(mount_table.db, callback);
    },
    add: function(mount_table, mount_id, ms_url, user, callback) {
        add_mount(mount_table.db, mount_id, ms_url, user, callback);
    },
    remove: function(mount_table, mount_id, callback) {
        remove_mount(mount_table.db, mount_id, callback);
    }
};
