import { GroupType } from "../const/socket-message-actions"
import ArrayUtil from "../utils/ArrayUtil"
import { ConnectionData } from "./SocketMessage"

export type DataRow = {
    dataRowId: string,
    clientId: string,
    userInfo: ConnectionData,
    prop: {
        group: GroupType,
        id: string
    },
}

export type DataRowQueue = {
    dataRowId: string,
    currentClientId: string,
    prop: {
        group: GroupType,
        id: string
    },
    queueClientIds: string[]
}

export class DataRowHelper {
    /**
     * removeClientFromLockedDataRows
     */
    public static removeClientFromDataRows(clientId: string, dataRows: DataRow[], group?: GroupType): [/*filtered*/DataRow[], /*removed*/DataRow[]] {
        const removedDataRows = dataRows.filter(dataRow => !!group ? dataRow.clientId === clientId && dataRow.prop.group === group : dataRow.clientId === clientId);
        const filteredDataRows = dataRows.filter(dataRow => !!group ? dataRow.clientId !== clientId && dataRow.prop.group === group : dataRow.clientId !== clientId);
        return [filteredDataRows, removedDataRows];
    }

    /**
     * removeClientFromLockedDataRowQueues
     */
    public static removeClientFromDataRowQueues(clientId: string, dataRowQueues: DataRowQueue[], group?: GroupType): DataRowQueue[] {
        const newDataRowQueues = dataRowQueues.filter(dataRowQueue => !(dataRowQueue.currentClientId === clientId && !dataRowQueue.queueClientIds.length))
        newDataRowQueues.forEach(dataRowQueue => {
            if (dataRowQueue.queueClientIds.length && (!!!group || dataRowQueue.prop.group === group)) ArrayUtil.removeElement(clientId, dataRowQueue.queueClientIds);
        })
        return newDataRowQueues;
    }
}