import { Request, Response } from 'express'; 
import WebSocket from 'ws';
import { app } from './src/configs/app';
import WebSocketController from './src/controllers/WebSocketController';
import { SocketMessage } from './src/types/SocketMessage';
import { SocketEventType, SocketMessageType } from './src/const/socket-message-actions';
import { v4 as uuidv4 } from 'uuid';
import { SAPEventListener } from './src/controllers/SAPEventListener';
import { livePool } from './src/configs/data-source';
import SSEController from './src/controllers/SSEController';

const port = process.env.PORT;

app.get('/', (req: Request, res: Response) => {
    res.send('Express + TypeScript Server');
});

const server = app.listen(port, () => {
    console.log(`⚡️[server]: Server is running at http://localhost:${port}`);
});

const sapEventListener: SAPEventListener = new SAPEventListener(livePool);
sapEventListener.run();
SSEController.run();

//initialize the WebSocket server instance
const wsServer = new WebSocket.Server({ server });

wsServer.on('connection', (ws: WebSocket) => {

    // initialize
    const clientId = uuidv4();
    WebSocketController.addWsEntity(clientId, ws);
    //connection is up, let's add a simple simple event
    ws.on('message', WebSocketController.handleMessage(ws));

    ws.on("close", WebSocketController.cleanUp(clientId));

    //send immediatly a feedback to the incoming connection  
    const socketMessage: SocketMessage =  {
        sessionId: '',
        type: SocketMessageType.MESSAGE,
        data: {
            clientId: clientId
        },
        event: SocketEventType.CONNECTION_ESTABLISHED
    }
    ws.send(JSON.stringify(socketMessage));
});