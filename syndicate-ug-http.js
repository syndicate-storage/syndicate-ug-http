#!/bin/env node
/*
   Copyright 2015 The Trustees of Princeton University

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

var rest = require('./rest.js');
var utils = require('./utils.js');
var express = require('express');
var app = express();

(function main() {
    var args = process.argv.slice(1);
    var param = utils.parse_args(args);

    console.log("Syndicate-UG-HTTP start");
    try {
        // start restfs
        app.use(function(req, res, next) {
            console.log('%s %s', req.method, req.url);
            next();
        });

        app.use('/', rest());

        app.listen(param.port, function() {
            console.log("listening at " + param.port);
        });
    } catch (e) {
        console.log("Exception occured: " + e);
    }
})();
