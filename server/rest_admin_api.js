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
var syndicateSetup = require('./syndicate_setup.js');
var async = require('async');
var fs = require('fs');


function make_mount_consistent(mount_table, session_table, mount_id, callback) {
    async.series({
        mount_table: function(cb) {
            mount_table.check(mount_id, function(err, result) {
                if(err) {
                    cb(util.format("could not check mount_table - %s", err), null);
                    return;
                }
                cb(null, result);
                return;
            });
        },
        syndicate_setup: function(cb) {
            syndicateSetup.check_user(mount_id, function(err, result) {
                if(err) {
                    cb(util.format("could not check syndicate_setup - %s", err), null);
                    return;
                }
                cb(null, result);
                return;
            });
        }
    }, function(err, results) {
        if(err) {
            callback(err, null);
            return;
        }

        if(results.mount_table && results.syndicate_setup) {
            // found
            callback(null, true);
            return;
        } else {
            if(results.mount_table) {
                mount_table.remove(mount_id, function(err_remove, data_remove) {
                    if(err_remove) {
                        callback(err_remove, null);
                        return;
                    }

                    session_table.remove_by_mount_id(mount_id, function(err_remove_session, data_remove_session) {
                        if(err_remove_session) {
                            callback(err_remove_session, null);
                            return;
                        }

                        callback(null, false);
                        return;
                    });
                    return;
                });
                return;
            } else if(results.syndicate_setup) {
                syndicateSetup.remove_user(mount_id, function(err_remove, data_remove) {
                    if(err_remove) {
                        callback(err_remove, null);
                        return;
                    }

                    callback(null, false);
                    return;
                });
                return;
            } else {
                callback(null, false);
                return;
            }
        }
    });
}

function admin_user_check(mount_table, session_table, mount_id, callback) {
    try {
        make_mount_consistent(mount_table, session_table, mount_id, function(err, result) {
            if(err) {
                callback(err, null);
                return;
            }

            var ret_data = {
                result: result,
                mount_id: mount_id
            };
            callback(null, ret_data);
            return;
        });
    } catch (ex) {
        callback(ex, null);
    }
}

function admin_user_setup(mount_table, session_table, mount_id, ms_url, user, user_cert, callback) {
    utils.log_debug(util.format("setting up a user - U(%s) / MountID(%s)", user, ms_url, mount_id));
    async.waterfall([
        function(cb) {
            make_mount_consistent(mount_table, session_table, mount_id, function(err, result) {
                if(err) {
                    cb(err, null);
                    return;
                }

                if(result) {
                    // exist
                    cb(util.format("user is already setup - U(%s) / MountID(%s)", user, mount_id), null);
                    return;
                }
                cb(null, false);
            });
        },
        function(result, cb) {
            // regist to mount_table
            mount_table.add(mount_id, ms_url, user, function(err, data) {
                if(err) {
                    cb(err, null);
                    return;
                }
                cb(null, data);
            });
        },
        function(result, cb) {
            utils.write_temp_file(user_cert, function(err, cert_path) {
                if(err) {
                    cb(err, null);
                    return;
                }
                cb(null, cert_path);
            });
        },
        function(cert_path, cb) {
            // register user (import)
            syndicateSetup.setup_user(mount_id, ms_url, user, cert_path, function(err, data) {
                if(err) {
                    cb(err, null);
                    return;
                }
                cb(null, cert_path);
            });
        },
        function(cert_path, cb) {
            // remove cert after installing it
            try {
                fs.unlink(cert_path, function(err, data) {
                    if(err) {
                        cb(err, null);
                        return;
                    }
                    cb(null, data);
                });
            } catch (ex) {
                cb(ex, null);
            }
        }
    ], function(err, data) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, true);
        return;
    });
}

function admin_user_delete(mount_table, session_table, mount_id, callback) {
    utils.log_debug(util.format("deleting a user - U(%s) / MountID(%s)", user, mount_id));
    async.waterfall([
        function(cb) {
            // check session_table
            session_table.list_by_mount_id(mount_id, function(err, session_records) {
                if(err) {
                    cb(err, null);
                    return;
                }

                if(session_records.length > 0) {
                    // there are live sessions!
                    var live_sessions = [];
                    session_records.forEach(function(live_session) {
                        live_sessions.push(live_session.name);
                    });
                    cb(util.format("There are live sessions - %s", live_sessions), null);
                    return;
                } else {
                    cb(null, session_records);
                    return;
                }
            });
        },
        function(result, cb) {
            // unregist to mount_table
            mount_table.remove(mount_id, function(err, data) {
                if(err) {
                    cb(err, null);
                    return;
                }
                cb(null, data);
            });
        },
        function(result, cb) {
            // unregister user
            syndicateSetup.remove_user(mount_id, function(err, data) {
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

        callback(null, true);
        return;
    });
}

function admin_gateway_check(session_table, session_name, callback) {
    try {
        session_table.check(session_name, function(err, result) {
            if(err) {
                callback(err, null);
                return;
            }

            var ret_data = {
                result: result,
                session_name: session_name
            };
            callback(null, ret_data);
            return;
        });
    } catch (ex) {
        callback(ex, null);
    }
}

function admin_gateway_setup(mount_table, session_table, mount_id, session_name, session_key, volume, gateway, anonymous, gateway_cert, callback) {
    utils.log_debug(util.format("setting up a gateway - V(%s) / G(%s) / MountID(%s) / A(%s) / S(%s)", volume, gateway, mount_id, anonymous, session_name));
    async.waterfall([
        function(cb) {
            mount_table.check(mount_id, function(err, exist) {
                if(err) {
                    cb(err, null);
                    return;
                }

                if(!exist) {
                    cb(util.format("Mount %s does not exist!", mount_id), null);
                    return;
                }

                cb(null, true);
            });
        },
        function(result, cb) {
            session_table.check(session_name, function(err, exist) {
                if(err) {
                    cb(err, null);
                    return;
                }

                if(exist) {
                    cb(util.format("Session %s already exists!", session_name), null);
                    return;
                }

                cb(null, true);
            });
        },
        function(result, cb) {
            // register session
            session_table.add(session_name, session_key, mount_id, volume, gateway, anonymous, function(err, data) {
                if(err) {
                    cb(err, null);
                    return;
                }
                cb(null, true);
            });
        },
        function(result, cb) {
            utils.write_temp_file(gateway_cert, function(err, cert_path) {
                if(err) {
                    cb(err, null);
                    return;
                }
                cb(null, cert_path);
            });
        },
        function(cert_path, cb) {
            // register gateway
            syndicateSetup.setup_gateway(mount_id, volume, gateway, anonymous, cert_path, function(err, data) {
                if(err) {
                    cb(err, null);
                    return;
                }
                cb(null, cert_path);
            });
        },
        function(cert_path, cb) {
            // remove cert after installing it
            try {
                fs.unlink(cert_path, function(err, data) {
                    if(err) {
                        cb(err, null);
                        return;
                    }
                    cb(null, data);
                });
            } catch(ex) {
                cb(ex, null);
            }
        },
        function(result, cb) {
            // mount!
            try {
                session_table.mount(session_name, syndicateSetup.get_configuration_path(mount_id), function(err, data) {
                    if(err) {
                        cb(err, null);
                        return;
                    }
                    cb(null, data);
                });
            } catch (e) {
                cb(e, null);
            }
        }
    ], function(err, data) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, true);
        return;
    });
}

function admin_gateway_delete(session_table, session_name, session_key, callback) {
    utils.log_debug(util.format("deleting a gateway - S(%s)", session_name));
    async.waterfall([
        function(cb) {
            // check session_table
            session_table.check_key(session_name, session_key, function(err, check_key_result) {
                if(err) {
                    cb(err, null);
                    return;
                }

                // session_key is correct
                cb(null, check_key_result);
                return;
            });
        },
        function(result, cb) {
            // close gateway_state
            session_table.unmount(session_name, function(err, unmount_result) {
                if(err) {
                    cb(err, null);
                    return;
                }

                cb(null, unmount_result);
                return;
            });
        },
        function(result, cb) {
            // remove session_table
            session_table.remove(session_name, function(err, remove_result) {
                if(err) {
                    cb(err, null);
                    return;
                }

                cb(null, remove_result);
                return;
            });
        }
    ], function(err, data) {
        if(err) {
            callback(err, null);
            return;
        }

        callback(null, data);
        return;
    });
}


/**
 * Expose root class
 */
module.exports = {
    make_mount_consistent: function(mount_table, session_table, mount_id, callback) {
        make_mount_consistent(mount_table, session_table, mount_id, callback);
    },
    user_check: function(req, res) {
        var options = req.query;
        var session_table = req.session_table;
        var mount_table = req.mount_table;

        // either mount_id or <ms_url, user> must be given
        var mount_id = options.mount_id;
        var ms_url = options.ms_url;
        var user = options.user;

        if(mount_id === null) {
            // if mount_id is not given, ms_url and user must be given
            if(ms_url === null) {
                restUtils.return_badrequest(req, res, "invalid request parameters - ms_url is not given");
                return;
            }

            if(user === null) {
                restUtils.return_badrequest(req, res, "invalid request parameters - user is not given");
                return;
            }

            mount_id = mountTable.make_mount_id(ms_url, user);
        }

        admin_user_check(mount_table, session_table, mount_id, function(err, data) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            restUtils.return_data(req, res, data);
            return;
        });
    },
    user_setup: function(req, res) {
        var options = req.query;
        var session_table = req.session_table;
        var mount_table = req.mount_table;

        var ms_url = restUtils.get_post_param("ms_url", options, req.body);
        if(ms_url === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - ms_url is not given");
            return;
        }

        var user = restUtils.get_post_param("user", options, req.body);
        if(user === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - user is not given");
            return;
        }

        // optional
        var mount_id = restUtils.get_post_param("mount_id", options, req.body);
        if(mount_id === null) {
            mount_id = mountTable.make_mount_id(ms_url, user);
        }

        var user_cert = restUtils.get_post_param("cert", options, req.body);
        if(user_cert === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - cert is not given");
            return;
        }

        admin_user_setup(mount_table, session_table, mount_id, ms_url, user, user_cert, function(err, data) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            restUtils.return_boolean(req, res, true);
            return;
        });
    },
    user_delete: function(req, res) {
        var options = req.query;
        var session_table = req.session_table;
        var mount_table = req.mount_table;

        // either mount_id or <ms_url, user> must be given
        var mount_id = options.mount_id;
        var ms_url = options.ms_url;
        var user = options.user;

        if(mount_id === null) {
            // if mount_id is not given, ms_url and user must be given
            if(ms_url === null) {
                restUtils.return_badrequest(req, res, "invalid request parameters - ms_url is not given");
                return;
            }

            if(user === null) {
                restUtils.return_badrequest(req, res, "invalid request parameters - user is not given");
                return;
            }

            mount_id = mountTable.make_mount_id(ms_url, user);
        }

        admin_user_delete(mount_table, session_table, mount_id, function(err, data) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            restUtils.return_boolean(req, res, true);
            return;
        });
    },
    gateway_check: function(req, res) {
        var options = req.query;
        var session_table = req.session_table;

        var session_name = options.session_name;
        if(session_name === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - session_name is not given");
            return;
        }

        admin_gateway_check(session_table, session_name, function(err, data) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            restUtils.return_data(req, res, data);
            return;
        });
    },
    gateway_setup: function(req, res) {
        var options = req.query;
        var session_table = req.session_table;
        var mount_table = req.mount_table;

        var session_name = restUtils.get_post_param("session_name", options, req.body);
        if(session_name === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - session_name is not given");
            return;
        }

        var session_key = restUtils.get_post_param("session_key", options, req.body);
        if(session_key === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - session_key is not given");
            return;
        }

        var mount_id = restUtils.get_post_param("mount_id", options, req.body);
        var ms_url = restUtils.get_post_param("ms_url", options, req.body);
        var user = restUtils.get_post_param("user", options, req.body);

        if(mount_id === null) {
            if(ms_url === null) {
                restUtils.return_badrequest(req, res, "invalid request parameters - ms_url is not given");
                return;
            }

            if(user === null) {
                restUtils.return_badrequest(req, res, "invalid request parameters - user is not given");
                return;
            }

            mount_id = mountTable.make_mount_id(ms_url, user);
        }

        var volume = restUtils.get_post_param("volume", options, req.body);
        if(volume === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - volume is not given");
            return;
        }

        var gateway = restUtils.get_post_param("gateway", options, req.body);
        if(gateway === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - gateway is not given");
            return;
        }

        var anonymous_str = restUtils.get_post_param("anonymous", options, req.body);
        var anonymous = false;
        if(anonymous_str.trim() === "true") {
            anonymous = true;
        }

        var gateway_cert = null;
        if(!anonymous) {
            var gateway_cert = restUtils.get_post_param("cert", options, req.body);
            if(gateway_cert === null) {
                restUtils.return_badrequest(req, res, "invalid request parameters - cert is not given");
                return;
            }
        }

        admin_gateway_setup(mount_table, session_table, mount_id, session_name, session_key, volume, gateway, anonymous, gateway_cert, function(err, data) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            restUtils.return_boolean(req, res, true);
            return;
        });
    },
    gateway_delete: function(req, res) {
        var options = req.query;
        var session_table = req.session_table;

        var session_name = restUtils.get_post_param("session_name", options, req.body);
        if(session_name === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - session_name is not given");
            return;
        }

        var session_key = restUtils.get_post_param("session_key", options, req.body);
        if(session_key === null) {
            restUtils.return_badrequest(req, res, "invalid request parameters - session_key is not given");
            return;
        }

        admin_gateway_delete(session_table, session_name, session_key, function(err, data) {
            if(err) {
                restUtils.return_error(req, res, err);
                return;
            }

            restUtils.return_boolean(req, res, true);
            return;
        });
    }
};
