"use strict";
// TODO: needs to support committing checkpoints to some kind of state
// TODO: need to document stuff better
// TODO: support clients sending unassign if they want to gracefully shutdown
Object.defineProperty(exports, "__esModule", { value: true });
// includes
const tcp_comm_1 = require("tcp-comm");
/* tslint:enable */
// define server logic
class PartitionerServer extends tcp_comm_1.TcpServer {
    constructor(options) {
        super(options);
        this.partitions = [];
        this.isLearning = false;
        // options or defaults
        this.options = options || {};
        if (options) {
            const local = this.options;
            local.rebalance = tcp_comm_1.TcpServer.toInt(local.rebalance);
            local.imbalance = tcp_comm_1.TcpServer.toInt(local.imbalance);
        }
    }
    get rebalanceEvery() {
        const local = this.options;
        return local.rebalance || 10000;
    }
    get allowedImbalance() {
        const local = this.options;
        return local.imbalance || 0;
    }
    get learnFor() {
        const local = this.options;
        return local.learnFor || 60000;
    }
    listen() {
        // once listening has started, start timed processes
        this.on('listen', () => {
            // start rebalancing
            this.scheduleRebalance();
            // start learning
            if (this.learnFor > 0) {
                this.emit('learning');
                this.isLearning = true;
                setTimeout(() => {
                    this.isLearning = false;
                    this.emit('managing');
                }, this.learnFor);
            }
            else {
                this.emit('managing');
            }
        });
        // when clients connect, give them all their current partitions
        this.on('connect', client => {
            if (client.socket) {
                // on connect, send an list of partitions that are currently assigned
                const partitions = this.partitions.reduce((array, partition) => {
                    if (partition.client === client) {
                        array.push({
                            id: partition.id,
                            pointer: partition.pointer
                        });
                    }
                    return array;
                }, []);
                if (partitions.length > 0) {
                    this.sendCommand(client, 'assign', partitions);
                }
                // if learning, send a request to the client to tell of any partitions it knows of
                if (this.isLearning) {
                    this.sendCommand(client, 'ask', null);
                }
            }
        });
        // propogate more explict naming
        this.on('add', client => {
            this.emit('add-client', client);
        });
        this.on('remove', client => {
            this.emit('remove-client', client);
        });
        // accept partitions from clients during learning mode
        this.on('cmd:assign', (client, payload, respond) => {
            try {
                const partitions = Array.isArray(payload)
                    ? payload
                    : [payload];
                for (const partition of partitions) {
                    const existing = this.partitions.find(p => p.id === partition.id);
                    if (!existing) {
                        this.partitions.push(partition);
                        this.emit('add-partition', partition);
                        partition.client = client;
                        this.emit('assign', partition);
                    }
                    else if (!existing.client) {
                        existing.client = client;
                        this.emit('assign', existing);
                    }
                }
            }
            catch (error) {
                this.emit('error', error, 'assign');
            }
            if (respond)
                respond();
        });
        // start listening
        super.listen();
    }
    counts() {
        const counts = [];
        for (const client of this.clients) {
            const count = this.partitions.filter(p => {
                if (p.yieldTo) {
                    return p.yieldTo.id === client.id;
                }
                else {
                    return p.client && p.client.id === client.id;
                }
            }).length;
            counts.push({
                client,
                count
            });
        }
        return counts;
    }
    rebalance() {
        try {
            // TODO: right now if there are more clients than partitions, nothing rebalances
            //   this is due to (< min) where min is 0
            // do not rebalance while learning
            if (this.isLearning) {
                this.scheduleRebalance();
                return;
            }
            // clear any clients that are not connected
            for (const client of this.clients) {
                if (!client.socket) {
                    for (const partition of this.partitions) {
                        if (partition.client &&
                            partition.client.id === client.id) {
                            this.emit('unassign', partition);
                            partition.client = undefined;
                        }
                    }
                    this.remove(client);
                }
            }
            // remove any clients that are
            // if there are no clients; there is nothing to rebalance
            if (this.clients.length > 0) {
                // count all partitions using a client
                const counts = this.counts();
                // determine the minimum number that should be allocated per client
                const min = Math.floor(this.partitions.length / this.clients.length);
                // function to assign unassigned
                const assign = (partition) => {
                    counts.sort((a, b) => {
                        return a.count - b.count;
                    });
                    partition.client = counts[0].client;
                    this.emit('assign', partition);
                    counts[0].count++;
                    if (partition.client.socket) {
                        this.sendCommand(partition.client, 'assign', {
                            id: partition.id,
                            pointer: partition.id
                        });
                    }
                };
                // assign unassigned
                for (const partition of this.partitions) {
                    if (!partition.client)
                        assign(partition);
                }
                // function to reassign the last appearance of a superior
                const reassign = (to) => {
                    counts.sort((a, b) => {
                        return b.count - a.count;
                    });
                    for (let i = this.partitions.length - 1; i >= 0; i--) {
                        const partition = this.partitions[i];
                        if (!partition.yieldTo &&
                            partition.client === counts[0].client) {
                            partition.yieldTo = to;
                            this.emit('yield', partition);
                            counts[0].count--;
                            return;
                        }
                    }
                };
                // reassign as needed to ensure minimums
                //  NOTE: this uses yield, does not immediately assign
                for (const entry of counts) {
                    while (min - entry.count > this.allowedImbalance) {
                        reassign(entry.client);
                        entry.count++;
                    }
                }
                // yield for anything still outstanding (yield should be IDEMPOTENT)
                for (const partition of this.partitions) {
                    if (partition.yieldTo) {
                        this.yield(partition);
                    }
                }
            }
            // emit a successful rebalance
            this.emit('rebalance');
        }
        catch (error) {
            this.emit('error', error, 'rebalance');
        }
        this.scheduleRebalance();
    }
    addPartition(partition) {
        const existing = this.partitions.find(p => p.id === partition.id);
        if (!existing) {
            this.partitions.push(partition);
            this.emit('add-partition', partition);
        }
    }
    removePartition(partition) {
        const index = this.partitions.indexOf(partition);
        if (index > -1) {
            this.partitions.splice(index, 1);
            this.emit('remote-partition', partition);
        }
    }
    addClient(client) {
        super.add(client);
    }
    removeClient(client) {
        super.remove(client);
    }
    async yield(partition) {
        // unassign function
        const unassign = async () => {
            try {
                // ask the current client to give it up
                if (partition.client && partition.client.socket) {
                    const closed = await this.sendCommand(partition.client, 'unassign', {
                        id: partition.id
                    }, {
                        receipt: true
                    });
                    // unassign
                    this.emit('unassign', partition);
                    partition.client = undefined;
                    // the most up-to-date pointer is returned
                    return closed.find(p => p.id === partition.id);
                }
            }
            catch (error) {
                this.emit('error', error, 'reassign:unassign');
            }
            return undefined;
        };
        // assign function
        const assign = async () => {
            try {
                // ask the new client to take it
                if (partition.yieldTo && partition.yieldTo.socket) {
                    await this.sendCommand(partition.yieldTo, 'assign', {
                        id: partition.id,
                        pointer: partition.pointer
                    }, {
                        receipt: true
                    });
                    // assign
                    partition.client = partition.yieldTo;
                    partition.yieldTo = undefined;
                    this.emit('assign', partition);
                }
            }
            catch (error) {
                this.emit('error', error, 'reassign:assign');
            }
        };
        // unassign if possible, but if it isn't, continue anyway
        const updated = await unassign();
        if (updated && updated.pointer)
            partition.pointer = updated.pointer;
        await assign();
    }
    scheduleRebalance() {
        setTimeout(() => {
            this.rebalance();
        }, this.rebalanceEvery);
    }
}
exports.PartitionerServer = PartitionerServer;
