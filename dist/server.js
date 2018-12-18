"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
var cmd = require("commander");
var dotenv = require("dotenv");
var winston = __importStar(require("winston"));
var PartitionerServer_1 = require("./PartitionerServer");
// set env
dotenv.config();
// define options
cmd.option('-l, --log-level <s>', 'LOG_LEVEL. The minimum level to log (error, warn, info, verbose, debug, silly). Defaults to "info".', /^(error|warn|info|verbose|debug|silly)$/i)
    .option('-p, --port <s>', '[REQUIRED] PORT. The port that is presenting the tcp interface. Default is "8000".', parseInt)
    .option('-r, --rebalance <i>', '[REQUIRED] REBALANCE. The number of milliseconds between rebalancing the partitions. Default is "10000" (10 seconds).', parseInt)
    .option('-i, --imbalance <i>', '[REQUIRED] IMBALANCE. If set to "0" the partitions will distributed equally, with a greater number, REBALANCE will allow a client to have more partitions than its peers per this value. Default is "0".', parseInt)
    .option('-t, --timeout <i>', '[REQUIRED] TIMEOUT. A client must checkin within the timeout period in milliseconds or its partitions will be reassigned. Default is "30000".', parseInt)
    .option('-l, --learn-for <i>', '[REQUIRED] LEARN_FOR. The number of milliseconds after this application starts up that it will stay in learning mode. Default is "60000" (1 min).', parseInt)
    .parse(process.argv);
// globals
var LOG_LEVEL = cmd.logLevel || process.env.LOG_LEVEL || 'info';
var PORT = cmd.port || process.env.PORT;
var REBALANCE = cmd.rebalance || process.env.REBALANCE;
var IMBALANCE = cmd.imbalance || process.env.IMBALANCE;
var TIMEOUT = cmd.timeout || process.env.TIMEOUT;
var LEARN_FOR = cmd.learnFor || process.env.LEARN_FOR;
// start logging
var logColors = {
    debug: '\x1b[32m',
    error: '\x1b[31m',
    info: '',
    silly: '\x1b[32m',
    verbose: '\x1b[32m',
    warn: '\x1b[33m' // yellow
};
var transport = new winston.transports.Console({
    format: winston.format.combine(winston.format.timestamp(), winston.format.printf(function (event) {
        var color = logColors[event.level] || '';
        var level = event.level.padStart(7);
        return event.timestamp + " " + color + level + "\u001B[0m: " + event.message;
    }))
});
var logger = winston.createLogger({
    level: LOG_LEVEL,
    transports: [transport]
});
function setup() {
    return __awaiter(this, void 0, void 0, function () {
        var server_1;
        return __generator(this, function (_a) {
            try {
                console.log("LOG_LEVEL is \"" + LOG_LEVEL + "\".");
                server_1 = new PartitionerServer_1.PartitionerServer({
                    imbalance: IMBALANCE,
                    learnFor: LEARN_FOR,
                    port: PORT,
                    rebalance: REBALANCE,
                    timeout: TIMEOUT
                })
                    .on('listen', function () {
                    logger.info("listening on port " + server_1.port + ".");
                })
                    .on('learning', function () {
                    logger.info('this application is now in learning mode.');
                })
                    .on('managing', function () {
                    logger.info('this application is now actively managing partitions (not learning).');
                })
                    .on('connect', function (client) {
                    if (client.socket) {
                        logger.info("client \"" + client.id + "\" connected from \"" + client.socket.remoteAddress + "\".");
                    }
                    else {
                        logger.info("client \"" + client.id + "\" connected.");
                    }
                })
                    .on('disconnect', function (client) {
                    if (client) {
                        logger.info("client \"" + client.id + "\" disconnected.");
                    }
                    else {
                        logger.info("an unknown client disconnected.");
                    }
                })
                    .on('ack', function (ack, msg) {
                    logger.debug("msg: " + msg.c + ", ack: " + JSON.stringify(ack));
                })
                    .on('remove', function (client) {
                    logger.info("client \"" + client.id + "\" removed.");
                })
                    .on('timeout', function (client) {
                    logger.info("client \"" + client.id + "\" timed-out (lastCheckIn: " + client.lastCheckin + ", now: " + new Date().valueOf() + ", timeout: " + server_1.timeout + ").");
                })
                    .on('rebalance', function () {
                    logger.verbose('rebalanced...');
                    var counts = server_1.counts();
                    if (counts.length > 0) {
                        var str = counts.map(function (c) { return c.client.id + ": " + c.count; });
                        logger.verbose(str.join(', '));
                    }
                    else {
                        logger.verbose('no partitions are assigned to clients.');
                    }
                })
                    .on('assign', function (partition) {
                    logger.info("partition \"" + partition.id + "\" was assigned to client \"" + (partition.client ? partition.client.id : 'unknown') + "\".");
                })
                    .on('unassign', function (partition) {
                    logger.info("partition \"" + partition.id + "\" was unassigned from client \"" + (partition.client ? partition.client.id : 'unknown') + "\".");
                })
                    .on('yield', function (partition) {
                    if (partition.yieldTo && partition.client) {
                        logger.info("partition \"" + partition.id + "\" was yielded to \"" + partition.yieldTo.id + "\" from \"" + partition.client.id + "\".");
                    }
                })
                    .on('add-partition', function (partition) {
                    logger.info("partition \"" + partition.id + "\" was added.");
                })
                    .on('remove-partition', function (partition) {
                    logger.info("partition \"" + partition.id + "\" was removed.");
                })
                    .on('error', function (error, module) {
                    logger.error("there was an error raised in module \"" + module + "\"...");
                    logger.error(error.stack ? error.stack : error.message);
                });
                // log settings
                logger.info("PORT is \"" + server_1.port + "\".");
                logger.info("REBALANCE is \"" + server_1.rebalanceEvery + "\".");
                logger.info("IMBALANCE is \"" + server_1.allowedImbalance + "\".");
                logger.info("TIMEOUT is \"" + server_1.timeout + "\".");
                logger.info("LEARN_FOR is \"" + server_1.learnFor + "\".");
                // start listening
                server_1.listen();
                // add some partitions
                server_1.addPartition({ id: 'partition-A' });
                server_1.addPartition({ id: 'partition-B' });
                server_1.addPartition({ id: 'partition-C' });
            }
            catch (error) {
                logger.error(error.stack);
            }
            return [2 /*return*/];
        });
    });
}
// run setup
setup();
