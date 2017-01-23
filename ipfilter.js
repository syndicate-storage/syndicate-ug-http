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

var utils = require('./utils.js');
var ipfilter = require('express-ipfilter');
var fs = require('fs');
var rangeCheck = require('range_check');

var IPFILTER_WHITELIST_FILENAME = 'whitelist';

function expand_IPv6(arr) {
    var new_arr = [];
    for(var i=0;i<arr.length;i++) {
        if(typeof(arr[i]) === "string") {
            new_arr.push(arr[i]);
            if(rangeCheck.ver(arr[i]) === 4) {
                var v6ip = "::ffff:" + arr[i].trim();
                if(arr.indexOf(v6ip) < 0) {
                    new_arr.push(v6ip);
                }
            }
        } else if(arr[i] instanceof Array) {
            // ip range
            new_arr.push(arr[i]);
            if(arr[i].length == 2) {
                if(rangeCheck.ver(arr[i][0]) === 4 && rangeCheck.ver(arr[i][1]) === 4) {
                    var v6ip_begin = "::ffff:" + arr[i][0].trim();
                    var v6ip_end = "::ffff:" + arr[i][1].trim();
                    new_arr.push([v6ip_begin, v6ip_end]);
                }
            }
        }
    }
    return new_arr;
}

/**
 * Expose root class
 */
module.exports = {
    // read local whilelist
    get_white_list: function() {
        var whitelist = "";
        var list = [];

        try {
            whitelist = fs.readFileSync(IPFILTER_WHITELIST_FILENAME, 'utf8');
        } catch (e) {
            // set to default
            whitelist = "ALL";
        }

        if(utils.is_json_string(whitelist)) {
            var json_obj = JSON.parse(whitelist);
            list = json_obj;
        } else {
            list = whitelist.trim().split(/\r?\n/);
        }

        if(list.indexOf("localhost") >= 0 && list.indexOf("127.0.0.1") < 0) {
            list.push("127.0.0.1");
        }

        // handle default ipv6 conversion from ipv4
        return expand_IPv6(list);
    },
    // return express filter
    get_express_filter: function(whitelist) {
        if(whitelist.indexOf("ALL") >= 0) {
            // return filter
            return null;
        } else {
            // filter ip range
            return ipfilter(whitelist, {mode: 'allow'});
        }
    }
};
