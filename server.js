'use strict';

// Module imports
var express = require('express')
  , https = require('https')
  , async = require('async')
  , _ = require('lodash')
  , restify = require('restify-clients')
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

log.level = 'verbose';
log.stream = process.stdout;
log.timestamp = true;

const VERSION = "1.0"
;

const PROCESS   = "PROCESS"
    , REST      = "REST"
    , WEBSOCKET = "WEBSOCKET"
    , DB        = "DB"
;

const DBHOST         = "https://apex.wedoteam.io"
    , DBURI          = '/ords/pdb1/wedo/common'
    , STREAMINGSETUP = '/streaming/setup'
;

const pingInterval = 25000
    , pingTimeout  = 60000
;

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  log.info(PROCESS,"Uncaught Exception: " + err);
  log.info(PROCESS,"Uncaught Exception: " + err.stack);
});
// Detect CTRL-C
process.on('SIGINT', function() {
  log.info(PROCESS,"Caught interrupt signal");
  log.info(PROCESS,"Exiting gracefully");
  process.exit(2);
});
// Main handlers registration - END

// Initializing REST client BEGIN
var dbClient = restify.createJsonClient({
  url: DBHOST,
  connectTimeout: 10000,
  requestTimeout: 10000,
  retry: false,
  rejectUnauthorized: false,
  headers: {
    "content-type": "application/json",
    "accept": "application/json"
  }
});
// Initializing REST client END

var demozones = _.noop();

async.series( {
  splash: (next) => {
    log.info(PROCESS, "WEDO OCI Streaming - WebSockets Bridge - " + VERSION);
    log.info(PROCESS, "Author: Carlos Casares <carlos.casares@oracle.com>");
    next();
  },
  setup: (next) => {
    log.verbose(DB, "Retrieving streaming setup for all demozones");
    dbClient.get(DBURI + STREAMINGSETUP, (err, req, res, obj) => {
      if (err) {
        log.verbose(DB, "Error retrieving setup: %s", err.message);
        next(err);
        return;
      }
      var jBody = JSON.parse(res.body);
      demozones = _.cloneDeep(jBody.items);
      next();
    });
  },
  websocket: (next) => {
    async.eachSeries( demozones, (d, nextDemozone) => {
      var i = 0;
      var interval = undefined;
      d.app = express();
      d.server = https.createServer(options, d.app);
      d.io = require('socket.io')(d.server, {'pingInterval': pingInterval, 'pingTimeout': pingTimeout});
      d.io.on('connection', function (socket) {
        log.info(d.demozone,"Connected!!");
        socket.conn.on('heartbeat', function() {
          log.verbose(d.demozone,'heartbeat');
        });
        socket.on('disconnect', function () {
          log.info(d.demozone,"Socket disconnected");
          if (interval) { clearInterval(interval) };
        });
        socket.on('error', function (err) {
          log.error(d.demozone,"Error: " + err);
        });

        var msg = {
          key: "MADRID,3344,PIZZA ORDERED",
          value: "test message"
        }
        socket.emit('message', JSON.stringify(msg));
        /**
        interval = setInterval(() => {
          var msg = 'Message number ' + (++i);
          console.log("Emitting: " + msg)
          socket.emit('message', msg);
        }, 1000);
        **/
      });
      d.server.listen(d.websocketport, function() {
        log.info(WEBSOCKET,"Created WS server at port: " + d.websocketport + " for demozone: " + d.demozone);
        console.log(d);
        next();
      });
    }, (err) => {
      next(err);
    });
  }
}, (err, results) => {
  if (err) {
    log.error("Error during initialization: " + err);
  }
});
