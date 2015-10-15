var path = require('path');
var spawn = require('child_process').spawn;
var options = {};
var syndicatefs;
var restfs = require('./restFs.js')

const DEFAULT_PORT_NUMBER = 11010;
const DEFAULT_MOUNT_POINT_PREFIX = "/tmp/syndicate_ug/syndicate_ug_http_";

const CMD_ARGS_MOUNT_POINT = ["--mount"];
const CMD_ARGS_PORT = ["--port"];

function parseArgs() {
    var args = process.argv;
    var syndicate_opt = [];
    var i;
    var port;

    // set a default port
    options.port = DEFAULT_PORT_NUMBER;
    
    // a temporary mount path
    options.mount_path = DEFAULT_MOUNT_POINT_PREFIX + process.pid;

    // parse
    // start from 2 since [0] is "node" and [1] is "syndicateUG.js"
    for(i=2;i<args.length;i++) {
        if(CMD_ARGS_MOUNT_POINT.indexOf(args[i]) >= 0 && i+1 <args.length) {
            options.mount_path = args[i+1];
            options.mount_path = path.resolve(__dirname, options.mount_path);
            i++;
        } else if(CMD_ARGS_PORT.indexOf(args[i]) >= 0 && i+1 < args.length) {
            port = parseInt(args[i+1]);
            if(!isNaN(port)) {
                options.port = port;
                i++;
            }
        } else {
            if(i === args.length - 1 && args[i][0] !== '-') {
                port = parseInt(args[i]);
                if(!isNaN(port)) {
                    options.port = port;
                } else {
                    syndicate_opt.push(args[i]);
                }
            } else {
                syndicate_opt.push(args[i]);
            }
        }
    }

    if(isNaN(options.port)) {
        options.port = DEFAULT_PORT_NUMBER;
    }

    syndicate_opt.push(options.mount_path);

    options.syndicate = syndicate_opt;
}

function prepareMountPoint() {
    // remove temporary mount paths

    // make a directory
}

function handleSyndicateLogging(data) {
    console.log('' + data);
}

function tryFinishSyndicateGracefully() {
    if(syndicatefs != null) {
        setTimeout(function () {
            if(syndicatefs != null) {
                // kill child process
                console.log('killing child process');
                syndicatefs.kill('SIGINT');
                // set to null
                syndicatefs = null;
            }
        }, 1000);
    }
}

function detectSyndicateFsPath() {
    //TODO: need to return syndicateFs install path
    return "syndicatefs";
}

function exitHandler(options, err) {
    if (options.cleanup) {
        console.log('clean');
        tryFinishSyndicateGracefully();
    }

    if (err) {
        console.log(err.stack);
    }

    process.exit();
}

(function main() {
    parseArgs();

    console.log("Syndicate-UG-HTTP uses '" + options.mount_path + "' as a mountpoint");
    console.log("Syndicate-UG-HTTP opens a service port " + options.port);

    console.log("Syndicate-UG-HTTP start");
    try {
        console.log("spawning syndicatefs with");
        //console.log(JSON.stringify(options));

        // execute
        //syndicatefs = spawn(detectSyndicateFsPath(), options.syndicate);
        syndicatefs = spawn(detectSyndicateFsPath());
        
        // set event-handlers
        syndicatefs.stdout.on('data', handleSyndicateLogging);
        syndicatefs.stderr.on('data', handleSyndicateLogging);
        syndicatefs.on('exit', exitHandler.bind(null, {cleanup:true}));

        process.stdin.resume();

        // pipe stdin to syndicatefs if needed
        process.stdin.setEncoding('utf-8');
        syndicatefs.stdin.setEncoding('utf-8');
        process.stdin.pipe(syndicatefs.stdin);

        syndicatefs.on('exit', exitHandler.bind(null, {cleanup:true}));
        syndicatefs.on('uncaughtException', exitHandler.bind(null, {cleanup:true}));
        syndicatefs.on('SIGINT', exitHandler.bind(null, {cleanup:true}));
        
        // start RESTfs
        restfs.start({
            // Port designation
            port: options.port,
            // Base directory
            base: options.mount_path,
            servicename: "syndicateUG"
        });
    } catch (e) {
        console.log("Exception occured when starting file system: " + e);
    }
})();
