// TODO: needs to support committing checkpoints to some kind of state
// TODO: need to document stuff better

// includes
import { IClient, IMessage, ITcpServerOptions, TcpServer } from 'tcp-comm';
import IPartition from './IPartition';

export interface IPartitionerServerOptions extends ITcpServerOptions {
    rebalance?: number;
    imbalance?: number;
}

export interface ICount {
    client: IClient;
    count: number;
}

/* tslint:disable */
// I wish there was a way to not redeclare listen...error, but I haven't found a way
export declare interface PartitionerServer {
    on(event: string, listener: (...args) => void): this;
    on(event: 'assign', listener: () => void): this;
    on(event: 'unassign', listener: (client: IClient) => void): this;
    on(event: 'rebalance', listener: (client: IClient) => void): this;
    on(event: 'add-partition', listener: (partition: IPartition) => void): this;
    on(
        event: 'remove-partition',
        listener: (partition: IPartition) => void
    ): this;
    on(event: 'listen', listener: () => void): this;
    on(event: 'connect', listener: (client: IClient) => void): this;
    on(event: 'checkin', listener: (client: IClient) => void): this;
    on(event: 'disconnect', listener: (client?: IClient) => void): this;
    on(event: 'add', listener: (client: IClient) => void): this;
    on(event: 'add-client', listener: (client: IClient) => void): this;
    on(event: 'remove', listener: (client: IClient) => void): this;
    on(event: 'remove-client', listener: (client: IClient) => void): this;
    on(event: 'yield', listener: (partition: IPartition) => void): this;
    on(event: 'timeout', listener: (client: IClient) => void): this;
    on(
        event: 'data',
        listener: (payload: any, respond?: (response?: any) => void) => void
    ): this;
    on(event: 'ack', listener: (ack: IMessage, msg: IMessage) => void): this;
    on(
        event: 'encode',
        listener: (before: number, after: number) => void
    ): this;
    on(event: 'error', listener: (error: Error, module: string) => void): this;
}
/* tslint:enable */

// define server logic
export class PartitionerServer extends TcpServer {
    public partitions: IPartition[] = [];

    public constructor(options?: IPartitionerServerOptions) {
        super(options);

        // options or defaults
        this.options = options || {};
        if (options) {
            const local: IPartitionerServerOptions = this.options;
            local.rebalance = TcpServer.toInt(local.rebalance);
            local.imbalance = TcpServer.toInt(local.imbalance);
        }
    }

    public get rebalanceEvery() {
        const local: IPartitionerServerOptions = this.options;
        return local.rebalance || 10000;
    }

    public get allowedImbalance() {
        const local: IPartitionerServerOptions = this.options;
        return local.imbalance || 0;
    }

    public listen() {
        // once listening has started, start rebalancing
        this.on('listen', () => {
            setTimeout(() => {
                this.rebalance();
            }, this.rebalanceEvery);
        });

        // when clients connect, give them all their current partitions
        this.on('connect', client => {
            if (client.socket) {
                const partitions = this.partitions.reduce(
                    (array, partition) => {
                        if (partition.client === client) {
                            array.push({
                                id: partition.id,
                                pointer: partition.pointer
                            });
                        }
                        return array;
                    },
                    [] as IPartition[]
                );
                if (partitions.length > 0) {
                    this.sendCommand(client, 'assign', partitions);
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

        // start listening
        super.listen();
    }

    public counts() {
        const counts: ICount[] = [];
        for (const client of this.clients) {
            const count = this.partitions.filter(
                p => p.client && p.client.id === client.id
            ).length;
            counts.push({
                client,
                count
            });
        }
        return counts;
    }

    public rebalance() {
        try {
            // TODO: right now if there are more clients than partitions, nothing rebalances
            //   this is due to (< min) where min is 0

            // clear any clients that are not connected
            for (const client of this.clients) {
                if (!client.socket) {
                    for (const partition of this.partitions) {
                        if (
                            partition.client &&
                            partition.client.id === client.id
                        ) {
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
                const min = Math.floor(
                    this.partitions.length / this.clients.length
                );

                // function to assign unassigned
                const assign = (partition: IPartition) => {
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
                    if (!partition.client) assign(partition);
                }

                // function to reassign the last appearance of a superior
                const reassign = (to: IClient) => {
                    counts.sort((a, b) => {
                        return b.count - a.count;
                    });
                    for (let i = this.partitions.length - 1; i >= 0; i--) {
                        const partition = this.partitions[i];
                        if (
                            !partition.yieldTo &&
                            partition.client === counts[0].client
                        ) {
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
        } catch (error) {
            this.emit('error', error, 'rebalance');
        }
        setTimeout(() => {
            this.rebalance();
        }, this.rebalanceEvery);
    }

    public addPartition(partition: IPartition) {
        const existing = this.partitions.find(p => p.id === partition.id);
        if (!existing) {
            this.partitions.push(partition);
            this.emit('add-partition', partition);
        }
    }

    public removePartition(partition: IPartition) {
        const index = this.partitions.indexOf(partition);
        if (index > -1) {
            this.partitions.splice(index, 1);
            this.emit('remote-partition', partition);
        }
    }

    public addClient(client: IClient) {
        super.add(client);
    }

    public removeClient(client: IClient) {
        super.remove(client);
    }

    public async yield(partition: IPartition) {
        // unassign function
        const unassign = async () => {
            try {
                // ask the current client to give it up
                if (partition.client && partition.client.socket) {
                    const closed: IPartition[] = await this.sendCommand(
                        partition.client,
                        'unassign',
                        {
                            id: partition.id
                        },
                        {
                            receipt: true
                        }
                    );

                    // the most up-to-date pointer is returned
                    return closed.find(p => p.id === partition.id);
                }
            } catch (error) {
                this.emit('error', error, 'reassign:unassign');
            }
            return undefined;
        };

        // assign function
        const assign = async () => {
            try {
                if (partition.yieldTo && partition.yieldTo.socket) {
                    await this.sendCommand(
                        partition.yieldTo,
                        'assign',
                        {
                            id: partition.id,
                            pointer: partition.pointer
                        },
                        {
                            receipt: true
                        }
                    );
                }
            } catch (error) {
                this.emit('error', error, 'reassign:assign');
            }
        };

        // unassign if possible, but if it isn't, continue anyway
        const updated = await unassign();
        if (updated && updated.pointer) partition.pointer = updated.pointer;
        await assign();

        // update the yield property
        partition.client = partition.yieldTo;
        partition.yieldTo = undefined;
    }
}
