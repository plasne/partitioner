import { IClient, IMessage, ITcpServerOptions, TcpServer } from 'tcp-comm';
import IPartition from './IPartition';
export interface IPartitionerServerOptions extends ITcpServerOptions {
    rebalance?: number;
    imbalance?: number;
    learnFor?: number;
}
export interface ICount {
    client: IClient;
    count: number;
}
export declare interface PartitionerServer {
    on(event: string, listener: (client: IClient, payload: any, respond?: (response?: any) => void) => void): this;
    on(event: 'learning', listener: () => void): this;
    on(event: 'managing', listener: () => void): this;
    on(event: 'assign', listener: (partition: IPartition) => void): this;
    on(event: 'unassign', listener: (partition: IPartition) => void): this;
    on(event: 'rebalance', listener: (client: IClient) => void): this;
    on(event: 'add-partition', listener: (partition: IPartition) => void): this;
    on(event: 'remove-partition', listener: (partition: IPartition) => void): this;
    on(event: 'listen', listener: () => void): this;
    on(event: 'connect', listener: (client: IClient, metadata: any) => void): this;
    on(event: 'checkin', listener: (client: IClient, metadata: any) => void): this;
    on(event: 'disconnect', listener: (client?: IClient) => void): this;
    on(event: 'add', listener: (client: IClient) => void): this;
    on(event: 'add-client', listener: (client: IClient) => void): this;
    on(event: 'remove', listener: (client: IClient) => void): this;
    on(event: 'remove-client', listener: (client: IClient) => void): this;
    on(event: 'yield', listener: (partition: IPartition) => void): this;
    on(event: 'timeout', listener: (client: IClient) => void): this;
    on(event: 'data', listener: (client: IClient, payload: any, respond?: (response?: any) => void) => void): this;
    on(event: 'ack', listener: (ack: IMessage, msg: IMessage) => void): this;
    on(event: 'encode', listener: (before: number, after: number) => void): this;
    on(event: 'error', listener: (error: Error, module: string) => void): this;
}
export declare class PartitionerServer extends TcpServer {
    partitions: IPartition[];
    isLearning: boolean;
    constructor(options?: IPartitionerServerOptions);
    readonly rebalanceEvery: number;
    readonly allowedImbalance: number;
    readonly learnFor: number;
    listen(): void;
    counts(): ICount[];
    rebalance(): void;
    addPartition(partition: IPartition): void;
    removePartition(partition: IPartition): void;
    addClient(client: IClient): void;
    removeClient(client: IClient): void;
    yield(partition: IPartition): Promise<void>;
    private scheduleRebalance;
}
