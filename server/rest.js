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
var express = require('express');
var expressSession = require('express-session');
var querystring = require('querystring');
var fs = require('fs');
var path = require('path');
var multer  = require('multer');
var bodyParser = require('body-parser');
var async = require('async');
var syndicate = require('syndicate-storage');
var passport = require('passport');
var basicStrategy = require('passport-http').BasicStrategy;
var utils = require('./utils.js');
var sessionTable = require('./session_table.js');
var mountTable = require('./mount_table.js');
var gatewayState = require('./gateway_state.js');
var syndicateSetup = require('./syndicate_setup.js');
var restUtils = require('./rest_utils.js');
var restAdminAPI = require('./rest_admin_api.js');
var restSessionAPI = require('./rest_session_api.js');
var restFSAPI = require('./rest_fs_api.js');


var UPLOADS_PATH = '/tmp/syndicate-ug-http/uploads';

var storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, UPLOADS_PATH);
    },
    filename: function (req, file, cb) {
        cb(null, util.format("%s.bin", Date.now()));
    }
});
var upload = multer({ storage: storage });

var authenticate = passport.authenticate('basic');

function make_all_mount_consistent(mount_table, session_table, callback) {
    mount_table.list(function(err, mount_records) {
        if(err) {
            callback(err, null);
            return;
        }

        mount_ids = {};
        mount_records.forEach(function(mount_record) {
            mount_ids[mount_record.mount_id] = function(cb) {
                restAdminAPI.make_mount_consistent(mount_table, session_table, mount_record.mount_id, cb);
            };
        });

        async.parallel(mount_ids, function(err_parallel, results) {
            if(err_parallel) {
                callback(err, null);
                return;
            }

            callback(null, true);
            return;
        });
    });
}

function reload_all_gateway_states(session_table, callback) {
    session_table.list(function(err, session_records) {
        if(err) {
            callback(err, null);
            return;
        }

        sessions = {};
        session_records.forEach(function(session_record) {
            sessions[session_record.name] = function(cb) {
                session_table.mount(session_record.name, syndicateSetup.get_configuration_path(session_record.mount_id), cb);
            };
        });

        async.parallel(sessions, function(err_parallel, results) {
            if(err_parallel) {
                callback(err_parallel, null);
                return;
            }

            callback(null, true);
            return;
        });
    });
}

module.exports = {
    init: function(app, server_config, callback) {
        // init REST
        utils.log_info("init: initializing REST framework");

        // set session
        app.use(expressSession({
            secret: utils.generate_random_string(64),
            resave: false,
            saveUninitialized: true,
        }));

        app.use(bodyParser.urlencoded({
            extended: true
        }));

        utils.create_dir_recursively_sync(path.dirname(server_config.db_path));
        utils.create_dir_recursively_sync(UPLOADS_PATH);
        syndicateSetup.set_syndicate_conf_root(server_config.config_root);

        async.series({
            mount_table: function(cb) {
                utils.log_info("init: initializing mount_table");
                mountTable.init(server_config.db_path, function(err, mount_table) {
                    if(err) {
                        cb(util.format("could not init mount_table - %s", err), null);
                        return;
                    }
                    cb(null, mount_table);
                    return;
                });
            },
            session_table: function(cb) {
                utils.log_info("init: initializing session_table");
                sessionTable.init(server_config.db_path, function(err, session_table) {
                    if(err) {
                        cb(util.format("could not init session_table - %s", err), null);
                        return;
                    }
                    cb(null, session_table);
                    return;
                });
            }
        }, function(err, results) {
            if(err) {
                callback(err, null);
                return;
            }

            var mount_table = results.mount_table;
            var session_table = results.session_table;

            app.use(function(req, res, next) {
                req.mount_table = mount_table;
                req.session_table = session_table;
                next();
            });

            // authentication
            passport.use(new basicStrategy(function(username, password, cb) {
                session_table.authenticate(username, password, cb);
            }));
            passport.serializeUser(function(session, cb) {
                session_table.serialize(session, cb);
            });
            passport.deserializeUser(function(session_name, cb) {
                session_table.deserialize(session_name, cb);
            });

            app.use(passport.initialize());
            app.use(passport.session());

            make_all_mount_consistent(mount_table, session_table, function(err_consistent, result_consistent) {
                if(err_consistent) {
                    callback(err_consistent, null);
                    return;
                }

                reload_all_gateway_states(session_table, function(err_reload, result_reload) {
                    if(err_reload) {
                        callback(err_reload, null);
                        return;
                    }

                    callback(null, "succeed");
                    return;
                });
                return;
            });
            return;
        });
    },
    get_router: function() {
        var router = new express.Router();
        router.use(function(req, res, next) {
            utils.log_info(util.format("%s %s", req.method, req.url));
            req.target = querystring.unescape(req.path);
            next();
        });

        // admin apis
        router.get('/user/check', restAdminAPI.user_check);
        router.post('/user/setup', upload.single('cert'), restAdminAPI.user_setup);
        router.post('/user/delete', restAdminAPI.user_delete);
        router.put('/user/delete', restAdminAPI.user_delete);
        router.delete('/user/delete', restAdminAPI.user_delete);
        router.get('/gateway/check', restAdminAPI.gateway_check);
        router.post('/gateway/setup', upload.single('cert'), restAdminAPI.gateway_setup);
        router.post('/gateway/delete', restAdminAPI.gateway_delete);
        router.put('/gateway/delete', restAdminAPI.gateway_delete);
        router.delete('/gateway/delete', restAdminAPI.gateway_delete);

        // session operations -- get
        router.get('/sessions', restSessionAPI.get_handler);

        /*
         * From this point, authentication is required.
         */
        router.get('*', authenticate, restFSAPI.get_handler);
        router.post('*', authenticate, restFSAPI.post_handler);
        router.put('*', authenticate, restFSAPI.post_handler);
        router.delete('*', authenticate, restFSAPI.delete_handler);
        return router;
    }
};
