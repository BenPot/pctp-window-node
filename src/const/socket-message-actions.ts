export enum SocketMessageType {
    MESSAGE = 'MESSAGE',
    BROADCAST = 'BROADCAST',
}

export enum SocketEventType {
    REGISTRATION = 'REGISTRATION',
    CONNECTION_ESTABLISHED = 'CONNECTION_ESTABLISHED',
    CLEAN_UP = 'CLEAN_UP',
    RELEASE = 'RELEASE',
    UPDATE = 'UPDATE',
    ASK_TO_UNLOCK = 'ASK_TO_UNLOCK',
}

export enum SocketEventStatus {
    REQUEST = 'REQUEST',
    FAILED = 'FAILED',
}

export enum GroupType { 
    POD = 'pod',
    BILLING = 'billing',
    TP = 'tp',
    PRICING = 'pricing',
}