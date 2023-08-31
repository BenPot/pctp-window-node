import { LocalStorage } from "node-persist";

export type StorageEntity = {
    id: string;
    storage: LocalStorage;
}