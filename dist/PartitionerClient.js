"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const tcp_comm_1 = require("tcp-comm");
/* tslint:enable */
// define server logic
class PartitionerClient extends tcp_comm_1.TcpClient {
    constructor(options) {
        super(options);
        this.partitions = [];
        this.isUnsettled = false;
    }
    waitOnSettle() {
        return new Promise(resolve => {
            if (this.isUnsettled) {
                this.once('settle', () => {
                    resolve();
                });
            }
            else {
                resolve();
            }
        });
    }
    connect() {
        // handle "assign"
        this.on('cmd:assign', (payload, respond) => {
            try {
                const partitions = Array.isArray(payload) ? payload : [payload];
                for (const partition of partitions) {
                    const existing = this.partitions.find(p => p.id === partition.id);
                    if (!existing) {
                        this.partitions.push(partition);
                        this.emit('assign', partition);
                    }
                }
            }
            catch (error) {
                this.emit('error', error, 'assign');
            }
            if (respond)
                respond();
        });
        // handle "unassign"
        //  returns an array of partitions with the most up-to-date pointer
        this.on('cmd:unassign', async (payload, respond) => {
            const closed = [];
            try {
                const partitions = Array.isArray(payload)
                    ? payload
                    : [payload];
                // wait on fetch to get the final pointer point
                await this.waitOnSettle();
                // remove each
                for (const partition of partitions) {
                    const index = this.partitions.findIndex(p => p.id === partition.id);
                    if (index > -1) {
                        this.partitions.splice(index, 1);
                        this.emit('unassign', partition);
                        closed.push(partition);
                    }
                }
            }
            catch (error) {
                this.emit('error', error, 'unassign');
            }
            if (respond) {
                // respond(closed);
            }
        });
        // handle "ask" (for which partitions the client would like)
        this.on('cmd:ask', (_, respond) => {
            try {
                this.sendCommand('assign', this.partitions);
            }
            catch (error) {
                this.emit('error', error, 'unassign');
            }
            if (respond)
                respond(closed);
        });
        // connect
        super.connect();
    }
}
exports.PartitionerClient = PartitionerClient;
