"use strict";
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (Object.hasOwnProperty.call(mod, k)) result[k] = mod[k];
    result["default"] = mod;
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const cmd = require("commander");
const dotenv = require("dotenv");
const winston = __importStar(require("winston"));
const PartitionerServer_1 = require("./PartitionerServer");
// set env
dotenv.config();
// define options
cmd.option('-l, --log-level <s>', 'LOG_LEVEL. The minimum level to log (error, warn, info, verbose, debug, silly). Defaults to "info".', /^(error|warn|info|verbose|debug|silly)$/i)
    .option('-p, --port <s>', '[REQUIRED] PORT. The port that is presenting the tcp interface. Default is "8000".', parseInt)
    .option('-r, --rebalance <i>', '[REQUIRED] REBALANCE. The number of milliseconds between rebalancing the partitions. Default is "10000" (10 seconds).', parseInt)
    .option('-i, --imbalance <i>', '[REQUIRED] IMBALANCE. If set to "0" the partitions will distributed equally, with a greater number, REBALANCE will allow a client to have more partitions than its peers per this value. Default is "0".', parseInt)
    .option('-t, --timeout <i>', '[REQUIRED] TIMEOUT. A client must checkin within the timeout period in milliseconds or its partitions will be reassigned. Default is "30000".', parseInt)
    .parse(process.argv);
// globals
const LOG_LEVEL = cmd.logLevel || process.env.LOG_LEVEL || 'info';
const PORT = cmd.port || process.env.PORT;
const REBALANCE = cmd.rebalance || process.env.REBALANCE;
const IMBALANCE = cmd.imbalance || process.env.IMBALANCE;
const TIMEOUT = cmd.timeout || process.env.TIMEOUT;
// start logging
const logColors = {
    debug: '\x1b[32m',
    error: '\x1b[31m',
    info: '',
    silly: '\x1b[32m',
    verbose: '\x1b[32m',
    warn: '\x1b[33m' // yellow
};
const transport = new winston.transports.Console({
    format: winston.format.combine(winston.format.timestamp(), winston.format.printf(event => {
        const color = logColors[event.level] || '';
        const level = event.level.padStart(7);
        return `${event.timestamp} ${color}${level}\x1b[0m: ${event.message}`;
    }))
});
const logger = winston.createLogger({
    level: LOG_LEVEL,
    transports: [transport]
});
async function setup() {
    try {
        console.log(`LOG_LEVEL is "${LOG_LEVEL}".`);
        // define the connection
        const server = new PartitionerServer_1.PartitionerServer({
            imbalance: IMBALANCE,
            port: PORT,
            rebalance: REBALANCE,
            timeout: TIMEOUT
        })
            .on('listen', () => {
            logger.info(`listening on port ${server.port}.`);
        })
            .on('connect', client => {
            if (client.socket) {
                logger.info(`client "${client.id}" connected from "${client.socket.remoteAddress}".`);
            }
            else {
                logger.info(`client "${client.id}" connected.`);
            }
        })
            .on('disconnect', (client) => {
            if (client) {
                logger.info(`client "${client.id}" disconnected.`);
            }
            else {
                logger.info(`an unknown client disconnected.`);
            }
        })
            .on('ack', (ack, msg) => {
            logger.debug(`msg: ${msg.c}, ack: ${JSON.stringify(ack)}`);
        })
            .on('remove', client => {
            logger.info(`client "${client.id}" removed.`);
        })
            .on('timeout', client => {
            logger.info(`client "${client.id}" timed-out (lastCheckIn: ${client.lastCheckin}, now: ${new Date().valueOf()}, timeout: ${server.timeout}).`);
        })
            .on('rebalance', () => {
            logger.verbose('rebalanced...');
            const counts = server.counts();
            if (counts.length > 0) {
                const str = counts.map(c => `${c.client.id}: ${c.count}`);
                logger.verbose(str.join(', '));
            }
            else {
                logger.verbose('no partitions are assigned to clients.');
            }
        })
            .on('assign', (partition) => {
            logger.info(`partition "${partition.id}" was assigned to client "${partition.client ? partition.client.id : 'unknown'}".`);
        })
            .on('unassign', (partition) => {
            logger.info(`partition "${partition.id}" was unassigned from client "${partition.client ? partition.client.id : 'unknown'}".`);
        })
            .on('yield', partition => {
            if (partition.yieldTo && partition.client) {
                logger.info(`partition "${partition.id}" was yielded to "${partition.yieldTo.id}" from "${partition.client.id}".`);
            }
        })
            .on('add-partition', partition => {
            logger.info(`partition "${partition.id}" was added.`);
        })
            .on('remove-partition', partition => {
            logger.info(`partition "${partition.id}" was removed.`);
        })
            .on('error', (error, module) => {
            logger.error(`there was an error raised in module "${module}"...`);
            logger.error(error.stack ? error.stack : error.message);
        });
        // log settings
        logger.info(`PORT is "${server.port}".`);
        logger.info(`REBALANCE is "${server.rebalanceEvery}".`);
        logger.info(`IMBALANCE is "${server.allowedImbalance}".`);
        logger.info(`TIMEOUT is "${server.timeout}".`);
        // start listening
        server.listen();
        // add some partitions
        server.addPartition({ id: 'partition-A' });
        server.addPartition({ id: 'partition-B' });
        server.addPartition({ id: 'partition-C' });
    }
    catch (error) {
        logger.error(error.stack);
    }
}
// run setup
setup();
