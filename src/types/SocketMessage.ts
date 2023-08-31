import { SocketEventStatus, SocketEventType, SocketMessageType } from "../const/socket-message-actions"
import { GroupType } from "../const/socket-message-actions"

export type ConnectionData = {
    clientId: string
    userId?: string
    userName?: string
}

export type RegistrationData = {
    prop: {
        group: GroupType,
        ids: string[],
        idInfo: {
            id: string,
            userInfo: ConnectionData
        }[]
    }
}

export type UpdateData = {
    clientId: string,
    prop: {
        group: GroupType,
        ids: string[]
    }
}

export type ReleaseData = {
    status?: SocketEventStatus,
    prop: {
        group: GroupType,
        id: string
    },
    requestorInfo?: ConnectionData
}

export type SocketMessage = {
    sessionId: string
    type: SocketMessageType
    event: SocketEventType
    data: string | ConnectionData | RegistrationData | ReleaseData
}

export type DirectMessage = {
    clientId: string,
    socketMessage: SocketMessage
}

export const isDirectMessage = (obj: DirectMessage): obj is DirectMessage => {
    return 'clientId' in obj && 'socketMessage' in obj;
}

export type BroadcastMessage = {
    clientIds: string[],
    socketMessage: SocketMessage
}

export const isBroadcastMessage = (obj: BroadcastMessage): obj is BroadcastMessage => {
    return 'clientIds' in obj && 'socketMessage' in obj;
}