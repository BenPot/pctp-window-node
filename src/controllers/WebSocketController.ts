import WebSocketService from '../services/WebSocketService';
import WebSocket from 'ws';
import { BroadcastMessage, ConnectionData, DirectMessage, RegistrationData, SocketMessage, isBroadcastMessage, isDirectMessage } from '../types/SocketMessage';
import JSONUtil from '../utils/JSONUtil';
import { DataRow, DataRowQueue } from '../types/DataRow';
import { StorageFactory } from '../factory/StorageFactory';
import { PersistedStorge } from '../factory/storage-interface';

class WebSocketController {
    public static storageFactory: StorageFactory;
    public static lockedDataRows: PersistedStorge<DataRow>;
    public static dataRowQueues: PersistedStorge<DataRowQueue>;
    public static wsEntities: any = {};
    public static userEntities: any = {};

    constructor() {
        WebSocketController.storageFactory = new StorageFactory('webSocketStorage');
        WebSocketController.storageFactory.init().then(res => {
            WebSocketController.storageFactory.factory<DataRow>('lockedDataRows', []).then(storage => {
                WebSocketController.lockedDataRows = storage;
                WebSocketController.storageFactory.factory<DataRowQueue>('dataRowQueues', []).then(storage => WebSocketController.dataRowQueues = storage)
            })
        })
    }

    /**
     * addWsEntity
     */
    public addWsEntity(clientId: string, ws: WebSocket) {
        WebSocketController.wsEntities[clientId] = ws;
    }
    /**
     * handleMessage
     */
    public handleMessage(ws: WebSocket): (message: string) => void {
        return (message: string) => {
            const socketMessage = JSONUtil.tryParse(message) as SocketMessage | boolean;
            if (<boolean>socketMessage) {
                const realSocketMessage = <SocketMessage>socketMessage
                WebSocketService.processSocketMessage(
                    realSocketMessage,
                    {
                        clientId: (<ConnectionData>realSocketMessage.data).clientId,
                        lockedDataRows: WebSocketController.lockedDataRows,
                        registrationData: (<RegistrationData>realSocketMessage.data),
                        dataRowQueues: WebSocketController.dataRowQueues,
                    }
                ).then(result => {
                    if (Array.isArray(result)) {
                        (<DirectMessage[]>result).forEach(({ clientId, socketMessage }) => {
                            WebSocketController.wsEntities[clientId].send(JSON.stringify(socketMessage));
                        })
                    }
                    if (!!result && isBroadcastMessage(<BroadcastMessage>result)) {
                        const { clientIds, socketMessage } = <BroadcastMessage>result;
                        clientIds.forEach(clientId => {
                            WebSocketController.wsEntities[clientId].send(JSON.stringify(socketMessage));
                        })
                    } else if (!!result && isDirectMessage(<DirectMessage>result)) {
                        const { clientId, socketMessage } = <DirectMessage>result;
                        WebSocketController.wsEntities[clientId].send(JSON.stringify(socketMessage));
                    } else {
                        if (!!result) ws.send(JSON.stringify(<SocketMessage>result));
                    }
                })
            } else {
                console.log('received: %s', message);
                ws.send(`Hello, you sent -> ${message}`);
            }
        }
    }

    /**
     * cleanUp
     */
    public cleanUp(clientIdToClean: string): () => void {
        return () => {
            WebSocketService.cleanUpCLient(clientIdToClean, WebSocketController.lockedDataRows, WebSocketController.dataRowQueues).then(directMessages => {
                if (Array.isArray(directMessages)) {
                    (<DirectMessage[]>directMessages).forEach(({ clientId, socketMessage }) => {
                        WebSocketController.wsEntities[clientId].send(JSON.stringify(socketMessage));
                    })
                }
                delete WebSocketController.wsEntities[clientIdToClean];
            });
        }
    }
}

export default (new WebSocketController()) as WebSocketController;