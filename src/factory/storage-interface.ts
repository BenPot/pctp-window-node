import storage, { LocalStorage, WriteFileResult } from 'node-persist';

export interface PersistedStorge<T> {
    get(): Promise<T[]>;
    set(t: T[]): Promise<storage.WriteFileResult>;
}

export class WebSocketStorage<T> implements PersistedStorge<T> {
    private storage: LocalStorage;
    private key: string;
    constructor(storage: LocalStorage, key: string) {
        this.storage = storage;
        this.key = key;
    }
    get(): Promise<T[]> {
        return this.storage.getItem(this.key) as Promise<T[]>;
    }
    set(t: T[]): Promise<WriteFileResult> {
        return this.storage.setItem(this.key, t);
    }
}