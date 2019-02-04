import { IClient } from 'tcp-comm';

export default interface IPartition {
    id: string;
    client?: IClient;
    metadata?: any;
    pointer?: number;
    yieldTo?: IClient;
}
