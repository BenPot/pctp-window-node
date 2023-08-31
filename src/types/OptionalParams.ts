import { PersistedStorge } from "../factory/storage-interface"
import { DataRow, DataRowQueue } from "./DataRow"
import { RegistrationData } from "./SocketMessage"

export type WSServiceOptionalParams = {
    clientId?: string,
    lockedDataRows?: PersistedStorge<DataRow>,
    dataRowQueues?: PersistedStorge<DataRowQueue>,
    registrationData?: RegistrationData
}