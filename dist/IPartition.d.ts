import { IClient } from 'tcp-comm';
export default interface IPartition {
    id: string;
    client?: IClient;
    pointer?: number;
    yieldTo?: IClient;
}
