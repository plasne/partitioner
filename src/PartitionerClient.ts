// includes
import { IMessage, ITcpClientOptions, TcpClient } from 'tcp-comm';
import IPartition from './IPartition';

/* tslint:disable */
export declare interface PartitionerClient {
    on(
        event: string,
        listener: (payload: any, respond?: (response?: any) => void) => void
    ): this;
    on(event: 'assign', listener: (partition: IPartition) => void): this;
    on(event: 'unassign', listener: (partition: IPartition) => void): this;
    on(event: 'settle', listener: () => void): this;
    on(event: 'listen', listener: () => void): this;
    on(event: 'connect', listener: () => void): this;
    on(event: 'checkin', listener: () => void): this;
    on(event: 'disconnect', listener: () => void): this;
    on(event: 'timeout', listener: () => void): this;
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
export class PartitionerClient extends TcpClient {
    public partitions: IPartition[] = [];
    public isUnsettled: boolean = false;

    public constructor(options?: ITcpClientOptions) {
        super(options);
    }

    public waitOnSettle() {
        return new Promise(resolve => {
            if (this.isUnsettled) {
                this.once('settle', () => {
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }

    public unsettle() {
        this.isUnsettled = true;
    }

    public settle() {
        this.isUnsettled = false;
        this.emit('settle');
    }

    public connect() {
        // handle "assign"
        this.on('cmd:assign', (payload: IPartition | IPartition[], respond) => {
            try {
                const partitions = Array.isArray(payload) ? payload : [payload];
                for (const partition of partitions) {
                    const existing = this.partitions.find(
                        p => p.id === partition.id
                    );
                    if (!existing) {
                        this.partitions.push(partition);
                        this.emit('assign', partition);
                    }
                }
            } catch (error) {
                this.emit('error', error, 'assign');
            }
            if (respond) respond();
        });

        // handle "unassign"
        //  returns an array of partitions with the most up-to-date pointer
        this.on(
            'cmd:unassign',
            async (payload: IPartition | IPartition[], respond) => {
                const closed: IPartition[] = [];
                try {
                    const partitions = Array.isArray(payload)
                        ? payload
                        : [payload];

                    // wait on fetch to get the final pointer point
                    await this.waitOnSettle();

                    // remove each
                    for (const partition of partitions) {
                        const index = this.partitions.findIndex(
                            p => p.id === partition.id
                        );
                        if (index > -1) {
                            this.partitions.splice(index, 1);
                            this.emit('unassign', partition);
                            closed.push(partition);
                        }
                    }
                } catch (error) {
                    this.emit('error', error, 'unassign');
                }
                if (respond) respond(closed);
            }
        );

        // handle "ask" (for which partitions the client would like)
        this.on('cmd:ask', (_, respond) => {
            try {
                this.sendCommand('assign', this.partitions);
            } catch (error) {
                this.emit('error', error, 'unassign');
            }
            if (respond) respond(closed);
        });

        // connect
        super.connect();
    }
}
