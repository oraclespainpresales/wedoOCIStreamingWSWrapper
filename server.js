'use strict';

// Module imports
var express = require('express')
  , https = require('https')
  , async = require('async')
  , _ = require('lodash')
  , log = require('npmlog-ts')
  , fs = require('fs')
;

// Instantiate classes & servers
const wsURI     = '/socket.io'
    , restURI   = '/event/:eventname';

// ************************************************************************
// Main code STARTS HERE !!
// ************************************************************************

const options = {
  cert: fs.readFileSync("/u01/ssl/certificate.fullchain.crt").toString(),
  key: fs.readFileSync("/u01/ssl/certificate.key").toString()
};

log.stream = process.stdout;
log.timestamp = true;

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  log.info("","Uncaught Exception: " + err);
  log.info("","Uncaught Exception: " + err.stack);
});
// Detect CTRL-C
process.on('SIGINT', function() {
  log.info("","Caught interrupt signal");
  log.info("","Exiting gracefully");
  process.exit(2);
});
// Main handlers registration - END

log.level = 'verbose';

const pingInterval = 25000
    , pingTimeout  = 60000
;

async.series([
    function(next) {
      var d = {
        port: 2443
      };
      var i = 0;
      var interval = undefined;
      d.app = express();
      d.server = https.createServer(options, d.app);
      d.io = require('socket.io')(d.server, {'pingInterval': pingInterval, 'pingTimeout': pingTimeout});
      d.io.on('connection', function (socket) {
        log.info(d.name,"Connected!!");
        socket.conn.on('heartbeat', function() {
          log.verbose(d.name,'heartbeat');
        });
        socket.on('disconnect', function () {
          log.info(d.name,"Socket disconnected");
          if (interval) { clearInterval(interval) };
        });
        socket.on('error', function (err) {
          log.error(d.name,"Error: " + err);
        });
        /**
        interval = setInterval(() => {
          var msg = 'Message number ' + (++i);
          console.log("Emitting: " + msg)
          socket.emit('message', msg);
        }, 1000);
        **/
      });
      d.server.listen(d.port, function() {
        log.info("","Created WS server at port: " + d.port);
        next();
      });
    }
], function(err, results) {
  if (err) {
    log.error("", err.message);
    process.exit(2);
  }
});
