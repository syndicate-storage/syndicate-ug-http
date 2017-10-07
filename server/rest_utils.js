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
var utils = require('./utils.js');
var syndicate = require('syndicate-storage');


function make_error_object(ex) {
    if(ex instanceof Error) {
        return {
            result: false,
            name: ex.name,
            message: ex.message,
        };
    } else {
        return {
            result: false,
            name: "error",
            message: ex,
        };
    }
}

function return_data(req, res, data) {
    // return with HTTP 200 code
    res.status(200).send(data);

    if(data instanceof Buffer) {
        utils.log_info(util.format("Respond with data (code 200) > %d bytes", data.length));
    } else {
        utils.log_info("Respond with data (code 200)");
    }
}

function return_boolean(req, res, success) {
    var ret_data = {
        result: success
    };
    return_data(req, res, ret_data);
}

function return_error_raw(req, res, error_code, error) {
    // return with HTTP error code
    error_code = error_code || 404;
    res.status(error_code).send(error);

    if(error instanceof Buffer) {
        utils.log_info(util.format("Respond with error (code %d) > %d bytes", error_code, error.length));
    } else {
        utils.log_info(util.format("Respond with error (code %d)", error_code));
        utils.log_debug(util.format("> %s", JSON.stringify(error)));
    }
}

function return_forbidden(req, res, msg) {
    return_error_raw(req, res, 403, make_error_object(msg));
}

function return_badrequest(req, res, msg) {
    return_error_raw(req, res, 400, make_error_object(msg));
}

function return_error(req, res, ex) {
    if(ex instanceof syndicate.syndicate_error) {
        if(ex.extra === 2) {
            // ENOENT
            return_error_raw(req, res, 404, make_error_object(ex));
        } else {
            return_error_raw(req, res, 500, make_error_object(ex));
        }
    } else if(ex instanceof Error) {
        return_error_raw(req, res, 500, make_error_object(ex));
        utils.log_error(ex.stack);
    } else {
        return_error_raw(req, res, 500, make_error_object(ex));
    }
}

function get_post_param(param, options, body) {
    var param_val = null;

    if(options) {
        if(param in options) {
            param_val = options[param];
        }
    }

    if(body) {
        if(param in body) {
            param_val = body[param];
        }
    }

    if(param_val !== null && param_val.length !== 0) {
        return param_val;
    }

    return null;
}


/**
 * Expose root class
 */
module.exports = {
    make_error_object: make_error_object,
    return_data: return_data,
    return_boolean: return_boolean,
    return_error_raw: return_error_raw,
    return_forbidden: return_forbidden,
    return_badrequest: return_badrequest,
    return_error: return_error,
    get_post_param: get_post_param
};
