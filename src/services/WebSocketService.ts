import { GroupType, SocketEventStatus, SocketEventType, SocketMessageType } from "../const/socket-message-actions";
import { PersistedStorge } from "../factory/storage-interface";
import { DataRow, DataRowHelper, DataRowQueue } from "../types/DataRow";
import { WSServiceOptionalParams } from "../types/OptionalParams";
import { ConnectionData, RegistrationData, DirectMessage, SocketMessage, BroadcastMessage, UpdateData, ReleaseData } from "../types/SocketMessage";
import { v4 as uuidv4 } from 'uuid';

class WebSocketService {
    /**
     * processMessage
     */
    public async processSocketMessage(socketMessage: SocketMessage, params: WSServiceOptionalParams): Promise<void | SocketMessage | DirectMessage | DirectMessage[] | BroadcastMessage> {
        switch (socketMessage.type) {
            case SocketMessageType.MESSAGE:
                return this.processMessage(socketMessage, params);
            case SocketMessageType.BROADCAST:
                return this.processBroadcast(socketMessage, params);
            default:
                break;
        }
    }

    private async processBroadcast(socketMessage: SocketMessage, { clientId, lockedDataRows, dataRowQueues, registrationData }: WSServiceOptionalParams): Promise<void | BroadcastMessage>{
        switch (socketMessage.event) {
            case SocketEventType.UPDATE:
                return this.broadCastUpdate(socketMessage, <PersistedStorge<DataRowQueue>>dataRowQueues);
            default:
                break;
        }
    }

    private async broadCastUpdate({ data, sessionId }: SocketMessage, dataRowQueues: PersistedStorge<DataRowQueue>): Promise<BroadcastMessage> {
        const { prop: { group, ids }, clientId } = <UpdateData>data;
        const newDataRowQueues: DataRowQueue[] = await dataRowQueues.get();
        let recipientIds: string[] = [];
        newDataRowQueues.forEach(({prop, queueClientIds}) => {
            if (ids.includes(prop.id) && group === prop.group) {
                recipientIds = [...recipientIds, ...queueClientIds ]
            }
        })
        const sendMessage: SocketMessage = {
            sessionId: sessionId,
            type: SocketMessageType.MESSAGE,
            event: SocketEventType.UPDATE,
            data: <UpdateData>data
        }
        return {
            clientIds: [...new Set(recipientIds)].filter(recipientId => recipientId !== clientId), 
            socketMessage: sendMessage
        }
    }

    private async processMessage(socketMessage: SocketMessage, { clientId, lockedDataRows, dataRowQueues, registrationData }: WSServiceOptionalParams): Promise<void | SocketMessage | DirectMessage | DirectMessage[]> {
        switch (socketMessage.event) {
            case SocketEventType.CONNECTION_ESTABLISHED:
                break;
            case SocketEventType.ASK_TO_UNLOCK:
                return this.askToUnlock(socketMessage, <PersistedStorge<DataRow>>lockedDataRows, <PersistedStorge<DataRowQueue>>dataRowQueues);
            case SocketEventType.REGISTRATION:
                return this.processRegistration(socketMessage, <PersistedStorge<DataRow>>lockedDataRows, <PersistedStorge<DataRowQueue>>dataRowQueues);
            case SocketEventType.CLEAN_UP:
                return this.cleanUpCLient(<string>clientId, <PersistedStorge<DataRow>>lockedDataRows, <PersistedStorge<DataRowQueue>>dataRowQueues, registrationData?.prop.group);
            default:
                break;
        }
    }

    private async askToUnlock({ data, sessionId }: SocketMessage, lockedDataRows: PersistedStorge<DataRow>, dataRowQueues: PersistedStorge<DataRowQueue>): Promise<DirectMessage> {
        const { group, id } = (<ReleaseData>data).prop
        const { clientId: requestorId } = (<ConnectionData>data)
        const newLockedDataRows: DataRow[] = await lockedDataRows.get();
        const foundDataRow = newLockedDataRows.filter(({ prop }) => prop.id === id && prop.group === group);
        if (foundDataRow.length) {
            return {
                clientId: foundDataRow[0].clientId,
                socketMessage: {
                    sessionId: "",
                    type: SocketMessageType.MESSAGE,
                    event: SocketEventType.ASK_TO_UNLOCK,
                    data: {
                        status: SocketEventStatus.REQUEST,
                        prop: {
                            group: group,
                            id: id
                        },
                        requestorInfo: <ConnectionData>data
                    }
                }
            };
        }
        return {
            clientId: requestorId,
            socketMessage: {
                sessionId: sessionId,
                type: SocketMessageType.MESSAGE,
                event: SocketEventType.ASK_TO_UNLOCK,
                data: {
                    status: SocketEventStatus.FAILED,
                    prop: {
                        group: group,
                        id: id
                    }
                }
            }
        };
    }

    private async processRegistration({ data, sessionId }: SocketMessage, lockedDataRows: PersistedStorge<DataRow>, dataRowQueues: PersistedStorge<DataRowQueue>): Promise<SocketMessage> {
        const { group, ids } = (<RegistrationData>data).prop
        const { clientId } = (<ConnectionData>data)
        const lockedIds: string[] = [];
        const newLockedDataRows: DataRow[] = await lockedDataRows.get();
        const newDataRowQueues: DataRowQueue[] = await dataRowQueues.get();
        const idInfo: {
            id: string,
            userInfo: ConnectionData
        }[] = [];
        ids.forEach(id => {
            const dataRowId: string = uuidv4();
            const foundDataRow = newLockedDataRows.filter(({ prop }) => prop.id === id && prop.group === group);
            if (foundDataRow.length) {
                for (const dataRowQueue of newDataRowQueues) {
                    if (dataRowQueue.dataRowId === foundDataRow[0].dataRowId && !dataRowQueue.queueClientIds.includes(clientId)) {
                        dataRowQueue.queueClientIds.push(clientId);
                        break;
                    }
                }
                idInfo.push({
                    id: id,
                    userInfo: foundDataRow[0].userInfo
                });
                lockedIds.push(id)
            } else {
                newDataRowQueues.push({
                    dataRowId: dataRowId,
                    currentClientId: clientId,
                    prop: {
                        group: group,
                        id: id
                    },
                    queueClientIds: [],
                })
                newLockedDataRows.push({
                    dataRowId: dataRowId,
                    clientId: clientId,
                    userInfo: <ConnectionData>data,
                    prop: {
                        group: group,
                        id: id
                    },
                })
            }
        })
        await lockedDataRows.set(newLockedDataRows)
        await dataRowQueues.set(newDataRowQueues)
        const sendMessage: SocketMessage = {
            sessionId: sessionId,
            type: SocketMessageType.MESSAGE,
            event: SocketEventType.REGISTRATION,
            data: {
                prop: {
                    group: group,
                    ids: lockedIds,
                    idInfo: idInfo
                }
            }
        }
        return sendMessage
    }

    /**
     * cleanUpCLient
     */
    public async cleanUpCLient(clientId: string, lockedDataRows: PersistedStorge<DataRow>, dataRowQueues: PersistedStorge<DataRowQueue>, group?: GroupType): Promise<void | DirectMessage[]> {
        const [filteredDataRows, removedDataRows] = DataRowHelper.removeClientFromDataRows(clientId, await lockedDataRows.get(), group);
        await lockedDataRows.set(filteredDataRows)
        await dataRowQueues.set(DataRowHelper.removeClientFromDataRowQueues(clientId, await dataRowQueues.get(), group));
        const directMessages = await this.releaseRemovedDataRows(removedDataRows, lockedDataRows, dataRowQueues);
        if (directMessages.length) return directMessages; else return;
    }

    /**
     * releaseRemovedDataRows
     */
    private async releaseRemovedDataRows(removedDataRows: DataRow[], lockedDataRows: PersistedStorge<DataRow>, dataRowQueues: PersistedStorge<DataRowQueue>): Promise<DirectMessage[]> {
        const directMessages: DirectMessage[] = [];
        const newLockedDataRows: DataRow[] = await lockedDataRows.get();
        const newDataRowQueues: DataRowQueue[] = await dataRowQueues.get();
        removedDataRows.forEach(removedDataRow => {
            for (const dataRowQueue of newDataRowQueues) {
                if (removedDataRow.dataRowId === dataRowQueue.dataRowId && dataRowQueue.queueClientIds.length) {
                    const newDataRowId: string = uuidv4();
                    dataRowQueue.dataRowId = newDataRowId;
                    dataRowQueue.currentClientId = <string>dataRowQueue.queueClientIds.shift();
                    removedDataRow.dataRowId = dataRowQueue.dataRowId
                    removedDataRow.clientId = dataRowQueue.currentClientId
                    newLockedDataRows.push(removedDataRow);
                    const message: SocketMessage = {
                        sessionId: '',
                        type: SocketMessageType.MESSAGE,
                        event: SocketEventType.RELEASE,
                        data: {
                            prop: {
                                group: removedDataRow.prop.group,
                                id: removedDataRow.prop.id
                            }
                        }
                    }
                    directMessages.push({
                        clientId: removedDataRow.clientId,
                        socketMessage: message
                    })
                }
            }
        })
        await lockedDataRows.set(newLockedDataRows);
        await dataRowQueues.set(newDataRowQueues);
        return directMessages
    }
}

export default (new WebSocketService()) as WebSocketService;