import { IMessage, ITcpClientOptions, TcpClient } from 'tcp-comm';
import IPartition from './IPartition';
export declare interface PartitionerClient {
    on(event: string, listener: (payload: any, respond?: (response?: any) => void) => void): this;
    on(event: 'assign', listener: (partition: IPartition) => void): this;
    on(event: 'unassign', listener: (partition: IPartition) => void): this;
    on(event: 'settle', listener: () => void): this;
    on(event: 'listen', listener: () => void): this;
    on(event: 'connect', listener: () => void): this;
    on(event: 'checkin', listener: () => void): this;
    on(event: 'disconnect', listener: () => void): this;
    on(event: 'timeout', listener: () => void): this;
    on(event: 'data', listener: (payload: any, respond?: (response?: any) => void) => void): this;
    on(event: 'ack', listener: (ack: IMessage, msg: IMessage) => void): this;
    on(event: 'encode', listener: (before: number, after: number) => void): this;
    on(event: 'error', listener: (error: Error, module: string) => void): this;
}
export declare class PartitionerClient extends TcpClient {
    partitions: IPartition[];
    isUnsettled: boolean;
    constructor(options?: ITcpClientOptions);
    waitOnSettle(): Promise<{}>;
    unsettle(): void;
    settle(): void;
    connect(): void;
}
