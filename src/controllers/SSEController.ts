import sql from 'mssql';
import { StorageFactory } from "../factory/StorageFactory";
import { PersistedStorge } from "../factory/storage-interface";
import { Request, Response } from "express";
import { v4 as uuidv4 } from 'uuid';
import { EventId, Subscriber } from "../types/EventEntity";
import { SAPEventListener } from "./SAPEventListener";
import TimeUtil from '../utils/TimeUtil';

class SSEController {
    public static subscriberEntities: any = {};

    public async register(req: Request, res: Response) {
        const headers = {
            'Content-Type': 'text/event-stream',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache'
        };
    
        res.writeHead(200, headers);
        
        const subscriberId = uuidv4();  
        const data = `data: ${JSON.stringify({id: subscriberId})}\n\n`;
        res.write(data);
        SSEController.subscriberEntities[subscriberId] = res;
        const timer = setInterval(function(){
            const content = `data: ${JSON.stringify({ignorable: true, timeStamp: new Date().toISOString()})}\n\n`;
            res.write(content);
        }, 1000);
        req.on('close', async () => {
            clearInterval(timer);
            console.log(`${subscriberId} Connection closed`);
            delete SSEController.subscriberEntities[subscriberId];
        });
    }

    public async run() {
        while (true) {
            if (!!SSEController.subscriberEntities && Object.keys(SSEController.subscriberEntities).length) {
                console.log(`Broadcasting events to ${Object.keys(SSEController.subscriberEntities).length} client(s)`)
                // await ((livePool: sql.ConnectionPool, subscriberEntities: any): Promise<void> => {
                //     return new Promise((resolve, reject) => {
                //         livePool.connect().then(async function(pool: sql.ConnectionPool) {
                //             const fetchedIds = await SAPEventListener.getFetchedIdsToRefresh(pool, true);
                //             for (const id in subscriberEntities) {
                //                 if (Object.prototype.hasOwnProperty.call(SSEController.subscriberEntities, id)) {
                //                     const response = SSEController.subscriberEntities[id];
                //                     response.write(`data: ${JSON.stringify({ignorable: false, fetchedIdsToProcess: fetchedIds})}\n\n`)
                //                 }
                //             }
                //             resolve();
                //         }).catch(function (err) {
                //             console.error('Error creating connection pool', err)
                //             reject();
                //         });
                //     })
                // })(SAPEventListener.livePool, SSEController.subscriberEntities);
            } else {
                console.log('No clients connected')
            }
            await TimeUtil.timeout(2000);
        }
    }

    public async notifySubscribers(fetchedIds: EventId[]) {
        try {
            console.log(`Notifying ${Object.keys(SSEController.subscriberEntities).length} client(s)`)
            for (const id in SSEController.subscriberEntities) {
                if (Object.prototype.hasOwnProperty.call(SSEController.subscriberEntities, id)) {
                    const response = SSEController.subscriberEntities[id];
                    response.write(`data: ${JSON.stringify({ignorable: false, fetchedIdsToProcess: fetchedIds})}\n\n`)
                }
            } 
        } catch (error) {
            console.log(error)
        }
    }
}

export default (new SSEController()) as SSEController;