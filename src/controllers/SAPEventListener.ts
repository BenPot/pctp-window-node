import sql from 'mssql';
import TimeUtil, { Timer } from '../utils/TimeUtil';
import { EventId } from '../types/EventEntity';
import { StorageFactory } from '../factory/StorageFactory';
import { PersistedStorge } from '../factory/storage-interface';
import JSONUtil from '../utils/JSONUtil';
import ArrayUtil from '../utils/ArrayUtil';
import SSEController from './SSEController';

export class SAPEventListener {
    public static freshStart: boolean = true;
    public static storageFactory: StorageFactory;
    public static processedIdStorage: PersistedStorge<string>;
    public static livePool: sql.ConnectionPool;
    public static newProcessedIds: String[] = []; //`${id}-${serial}`
    public static newAdhocIds: EventId[] = []; // from clients update
    private readonly timeOut: number = 10000;
    private readonly numberOfIdsToProcessed: number = 1;
    private monitoringTime: number = (new Date()).setHours(0,0,0,0);
    constructor(livePool: sql.ConnectionPool) {
        SAPEventListener.livePool = livePool;
        while (true) {
            try {
                SAPEventListener.storageFactory = new StorageFactory('sapEventListener');
                break;
            } catch (error) {
                console.log('error on storage creation...', error)
            }
            console.log('retry storage creation...');
            (async ():Promise<void> => new Promise(async resolve => { await TimeUtil.timeout(this.timeOut); resolve(); }))()
        }
        SAPEventListener.storageFactory.init().then(res => {
            SAPEventListener.storageFactory.factory<string>('processedIds', []).then(storage => {
                SAPEventListener.processedIdStorage = storage;
                console.log(`SAPEventListener.processedIdStorage has been set.`)
            }).catch(err => console.log('error on storage creation...', err))
        }).catch(err => console.log('error on storage creation...', err))
    }
    public async run() {
        if (process.env.ENV !== 'prod') {
            while (true) {
                if (!!SAPEventListener.processedIdStorage) {
                    const fetchedIds: EventId[] = [];
                    const testEventIds: EventId[] | null = JSONUtil.parseObjectFromFile<EventId[]>('./test_data/event_ids.json');
                    if (!!testEventIds) {
                        testEventIds.forEach(eventId => fetchedIds.push(eventId as EventId));
                        const connectionPool: sql.ConnectionPool = await SAPEventListener.livePool.connect();
                        if (!!fetchedIds) await this.processFetchedIds(connectionPool, fetchedIds);
                    }
                }
                await TimeUtil.timeout(this.timeOut);
            }
        } else {
            while (true) {
                try {
                    if (!!SAPEventListener.processedIdStorage && !!SAPEventListener.livePool) {
                        if (this.monitoringTime !== (new Date()).setHours(0,0,0,0)) {
                            this.monitoringTime = (new Date()).setHours(0,0,0,0);
                            SAPEventListener.newProcessedIds.length = 0;
                            await SAPEventListener.processedIdStorage.set([]);
                        }
                        await ((livePool: sql.ConnectionPool, sapEventListener: SAPEventListener): Promise<void> => {
                            let fetchedIds: EventId[] = [];
                            return new Promise((resolve1, reject1) => {
                                livePool.connect().then(async function(pool: sql.ConnectionPool) {
                                    fetchedIds = await SAPEventListener.getFetchedIdsToRefresh(pool);
                                    if (!!fetchedIds) await sapEventListener.processFetchedIds(pool, fetchedIds);
                                    resolve1()
                                }).catch(function (err) {
                                    console.error('Error creating connection pool', err)
                                    reject1();
                                });
                            })
                        })(SAPEventListener.livePool, this);
                        await TimeUtil.timeout(this.timeOut)
                    } else {
                        await TimeUtil.timeout(2000)
                    }
                } catch (error) {
                    console.log('error caught', error)
                    await TimeUtil.timeout(2000)
                }
            }
        }
    }

    public static async getFetchedIdsToRefresh(pool: sql.ConnectionPool, doDisregardAlreadyProcessed: boolean = false): Promise<EventId[]> {
        const fetchedIds: EventId[] = [];
        const timer: Timer = new Timer();
        timer.reset();
        type QueryPromiseCallback = (resolve: (arg: EventId[]) => void, reject: () => void) => (err: Error | undefined, recordset: sql.IResult<unknown> | undefined) => Promise<void>
        const queryPromiseCallback: QueryPromiseCallback = (resolve: (arg: EventId[]) => void, reject: () => void) => async (err: Error | undefined, recordset: sql.IResult<unknown> | undefined) => {
            if (err) {
                console.log(err)
                reject();
            }
            if (!!recordset) {
                resolve(recordset.recordset as EventId[]);
            }
            reject();
        }
        const queryPromise = (pool: sql.ConnectionPool, query: string, queryPromiseCallback: QueryPromiseCallback) => new Promise((resolve, reject) => {
            pool.query(query, queryPromiseCallback(resolve, reject));
        });
        const apEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                TP.U_BookingNumber AS id,
                CONCAT('AP', head.DocNum, '-', FORMAT(head.UpdateDate, 'yyyyMMdd'), head.UpdateTS) AS serial
            FROM (
            SELECT DocEntry, DocNum, U_PVNo, UpdateDate, UpdateTS
            FROM OPCH WITH(NOLOCK)
            WHERE CAST(CreateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
            OR CAST(UpdateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
            ) head
            LEFT JOIN (
                SELECT DocEntry, ItemCode
                FROM PCH1 WITH(NOLOCK)
            ) line ON head.DocEntry = line.DocEntry
            LEFT JOIN (
            SELECT U_BookingId AS U_BookingNumber, U_PVNo
            FROM [@PCTP_TP] WITH(NOLOCK)
            ) TP ON 1 = CASE
                WHEN TP.U_PVNo = head.U_PVNo THEN 1
                WHEN head.U_PVNo IN (
                    SELECT 
                        RTRIM(LTRIM(value))
                    FROM STRING_SPLIT(TP.U_PVNo, ',')
                ) THEN 1
                WHEN TP.U_PVNo IN (
                    SELECT 
                        RTRIM(LTRIM(value))
                    FROM STRING_SPLIT(head.U_PVNo, ',')
                ) THEN 1
                ELSE 0
            END OR TP.U_BookingNumber = line.ItemCode
            WHERE TP.U_BookingNumber IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const opEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                UNI.U_BookingNumber AS id,
                CONCAT('OP', OP.DocNum, '-', FORMAT(OP.UpdateDate, 'yyyyMMdd'), OP.UpdateTS) AS serial
            FROM (
            SELECT U_BookingNumber, tp_U_DocNum, U_Paid
            FROM PCTP_UNIFIED WITH(NOLOCK)
            ) UNI,
            (
                SELECT T0.DocNum, T0.UpdateDate, T0.UpdateTS, T3.DocNum AS ApDocNum
                FROM OVPM T0 WITH(NOLOCK)
                INNER JOIN VPM2 T1 ON T1.DocNum = T0.DocEntry
                LEFT JOIN VPM1 T2 ON T1.DocNum = T2.DocNum
                LEFT JOIN OPCH T3 ON T1.DocEntry = T3.DocEntry
                WHERE T0.Canceled <> 'Y' 
                AND (
                    CAST(T0.CreateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
                    OR CAST(T0.UpdateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
                )
            ) OP
            WHERE UNI.U_BookingNumber IS NOT NULL AND UNI.U_BookingNumber <> ''
            AND (
                OP.ApDocNum IN (SELECT RTRIM(LTRIM(value)) FROM STRING_SPLIT(UNI.tp_U_DocNum, ','))
                OR OP.ApDocNum IN (SELECT RTRIM(LTRIM(value)) FROM STRING_SPLIT(UNI.U_Paid, ','))
            )
        `, queryPromiseCallback) as Promise<EventId[]>
        const soEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                line.ItemCode AS id,
                CONCAT('SO', head.DocNum, '-', FORMAT(head.UpdateDate, 'yyyyMMdd'), head.UpdateTS) AS serial
            FROM (
            SELECT DocEntry, DocNum, UpdateDate, UpdateTS
            FROM ORDR WITH(NOLOCK)
            WHERE CAST(CreateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
            OR CAST(UpdateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
            ) head
            LEFT JOIN (
                SELECT DocEntry, ItemCode
                FROM RDR1 WITH(NOLOCK)
            ) line ON head.DocEntry = line.DocEntry
            WHERE line.ItemCode IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const arEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                line.ItemCode AS id,
                CONCAT('AR', head.DocNum, '-', FORMAT(head.UpdateDate, 'yyyyMMdd'), head.UpdateTS) AS serial
            FROM (
            SELECT DocEntry, DocNum, UpdateDate, UpdateTS
            FROM OINV WITH(NOLOCK)
            WHERE CAST(CreateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
            OR CAST(UpdateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
            ) head
            LEFT JOIN (
                SELECT DocEntry, ItemCode
                FROM INV1 WITH(NOLOCK)
            ) line ON head.DocEntry = line.DocEntry
            WHERE line.ItemCode IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const bnEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT 
                head.ItemCode AS id,
                CONCAT('BN-', FORMAT(head.CreateDate, 'yyyyMMdd'), head.CreateTS) AS serial
            FROM (
                SELECT ItemCode, CreateDate, CreateTS
                FROM OITM WITH(NOLOCK) 
            ) head
            WHERE head.ItemCode IS NOT NULL
            AND CAST(head.CreateDate AS date) = CAST(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'} AS date)
            AND (
                EXISTS(SELECT 1 FROM [@PCTP_POD] WITH(NOLOCK) WHERE U_BookingNumber = head.ItemCode)
                OR EXISTS(SELECT 1 FROM [@PCTP_PRICING] WITH(NOLOCK) WHERE U_BookingId = head.ItemCode)
            )
            AND (
                NOT EXISTS(SELECT 1 FROM PCTP_UNIFIED WITH(NOLOCK) WHERE U_BookingNumber = head.ItemCode)
            )
        `, queryPromiseCallback) as Promise<EventId[]>
        const dupEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT 
                U_BookingNumber AS id,
                CONCAT('DUP-', FORMAT(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'}, 'yyyyMMddhhmmss')) AS serial
            FROM PCTP_UNIFIED WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
        `, queryPromiseCallback) as Promise<EventId[]>
        // const sbdiEvent: Promise<EventId[]> = queryPromise(pool, `
        //     SELECT
        //         SE.U_BookingNumber as id,
        //         CONCAT('SBDI-', FORMAT(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'}, 'yyyyMMddhhmmss')) AS serial
        //     FROM (
        //         SELECT U_BookingNumber, U_BillingStatus, U_InvoiceNo, U_PODSONum, U_ARDocNum
        //         FROM SUMMARY_EXTRACT WITH(NOLOCK)
        //     ) SE
        //     LEFT JOIN (
        //         SELECT U_BookingId, U_BillingStatus, U_InvoiceNo, U_PODSONum, U_DocNum
        //         FROM BILLING_EXTRACT WITH(NOLOCK)
        //     ) BE ON BE.U_BookingId = SE.U_BookingNumber
        //     WHERE ((
        //         (BE.U_BillingStatus <> SE.U_BillingStatus AND REPLACE(BE.U_BillingStatus, ' ', '') <> REPLACE(SE.U_BillingStatus, ' ', '')) 
        //         OR (
        //             (
        //                 BE.U_BillingStatus IS NOT NULL AND REPLACE(BE.U_BillingStatus, ' ', '') <> '' 
        //                 AND (SE.U_BillingStatus IS NULL OR REPLACE(SE.U_BillingStatus, ' ', '') = '')
        //             )
        //             OR (
        //                 SE.U_BillingStatus IS NOT NULL AND REPLACE(SE.U_BillingStatus, ' ', '') <> '' 
        //                 AND (BE.U_BillingStatus IS NULL OR REPLACE(BE.U_BillingStatus, ' ', '') = '')
        //             )
        //         )
        //     )
        //     OR (
        //         (BE.U_InvoiceNo <> SE.U_InvoiceNo AND REPLACE(BE.U_InvoiceNo, ' ', '') <> REPLACE(SE.U_InvoiceNo, ' ', '')) 
        //         OR (
        //             (
        //                 BE.U_InvoiceNo IS NOT NULL AND REPLACE(BE.U_InvoiceNo, ' ', '') <> '' 
        //                 AND (SE.U_InvoiceNo IS NULL OR REPLACE(SE.U_InvoiceNo, ' ', '') = '')
        //             )
        //             OR (
        //                 SE.U_InvoiceNo IS NOT NULL AND REPLACE(SE.U_InvoiceNo, ' ', '') <> '' 
        //                 AND (BE.U_InvoiceNo IS NULL OR REPLACE(BE.U_InvoiceNo, ' ', '') = '')
        //             )
        //         )
        //     )
        //     OR (
        //         (BE.U_PODSONum <> SE.U_PODSONum AND REPLACE(BE.U_PODSONum, ' ', '') <> REPLACE(SE.U_PODSONum, ' ', '')) 
        //         OR (
        //             (
        //                 BE.U_PODSONum IS NOT NULL AND REPLACE(BE.U_PODSONum, ' ', '') <> '' 
        //                 AND (SE.U_PODSONum IS NULL OR REPLACE(SE.U_PODSONum, ' ', '') = '')
        //             )
        //             OR (
        //                 SE.U_PODSONum IS NOT NULL AND REPLACE(SE.U_PODSONum, ' ', '') <> '' 
        //                 AND (BE.U_PODSONum IS NULL OR REPLACE(BE.U_PODSONum, ' ', '') = '')
        //             )
        //         )
        //     )
        //     OR (
        //         (BE.U_DocNum <> SE.U_ARDocNum AND REPLACE(BE.U_DocNum, ' ', '') <> REPLACE(SE.U_ARDocNum, ' ', '')) 
        //         OR (
        //             (
        //                 BE.U_DocNum IS NOT NULL AND REPLACE(BE.U_DocNum, ' ', '') <> '' 
        //                 AND (SE.U_ARDocNum IS NULL OR REPLACE(SE.U_ARDocNum, ' ', '') = '')
        //             )
        //             OR (
        //                 SE.U_ARDocNum IS NOT NULL AND REPLACE(SE.U_ARDocNum, ' ', '') <> '' 
        //                 AND (BE.U_DocNum IS NULL OR REPLACE(BE.U_DocNum, ' ', '') = '')
        //             )
        //         )
        //     ))
        //     AND BE.U_BookingId IS NOT NULL AND SE.U_BookingNumber IS NOT NULL
        // `, queryPromiseCallback) as Promise<EventId[]>
        // const bvnrEvent: Promise<EventId[]> = queryPromise(pool, `
        //     SELECT
        //         T0.U_BookingNumber as id,
        //         CONCAT('B-TBVNR-', FORMAT(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'}, 'yyyyMMddhhmmss')) AS serial
        //     FROM (
        //         SELECT U_BookingNumber, U_PODStatusDetail
        //         FROM [dbo].[@PCTP_POD] WITH(NOLOCK)
        //     ) T0
        //     WHERE 1=1
        //     AND (CAST(T0.U_PODStatusDetail as nvarchar(100)) LIKE '%Verified%' OR CAST(T0.U_PODStatusDetail as nvarchar(100)) LIKE '%ForAdvanceBilling%')
        //     AND T0.U_BookingNumber NOT IN (SELECT U_BookingId FROM BILLING_EXTRACT WITH(NOLOCK))
        //     AND T0.U_BookingNumber IN (SELECT U_BookingId FROM [dbo].[@PCTP_BILLING] WITH(NOLOCK) WHERE U_BookingId IS NOT NULL)
        //     AND T0.U_BookingNumber IS NOT NULL
        // `, queryPromiseCallback) as Promise<EventId[]>
        // const tvnrEvent: Promise<EventId[]> = queryPromise(pool, `
        //     SELECT
        //         T0.U_BookingNumber as id,
        //         CONCAT('T-TBVNR-', FORMAT(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'}, 'yyyyMMddhhmmss')) AS serial
        //     FROM [dbo].[@PCTP_POD] T0 WITH(NOLOCK)
        //     WHERE 1=1
        //     AND (CAST(T0.U_PODStatusDetail as nvarchar(100)) LIKE '%Verified%')
        //     AND T0.U_BookingNumber NOT IN (SELECT U_BookingId FROM TP_EXTRACT WITH(NOLOCK) WHERE U_BookingId IS NOT NULL)
        //     AND T0.U_BookingNumber IN (SELECT U_BookingId FROM [dbo].[@PCTP_TP] WITH(NOLOCK) WHERE U_BookingId IS NOT NULL)
        //     AND T0.U_BookingNumber IS NOT NULL
        // `, queryPromiseCallback) as Promise<EventId[]>
        // const bpdiEvent: Promise<EventId[]> = queryPromise(pool, `
        //     SELECT
        //         BE.U_BookingId AS id,
        //         CONCAT('B-BTPDI-', FORMAT(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'}, 'yyyyMMddhhmmss')) AS serial
        //     FROM (
        //         SELECT U_BookingId, U_GrossInitialRate, U_Demurrage, U_AddCharges
        //         FROM BILLING_EXTRACT WITH(NOLOCK)
        //     ) BE
        //     LEFT JOIN (
        //         SELECT U_BookingId, U_GrossClientRatesTax, U_Demurrage, U_TotalAddtlCharges
        //         FROM PRICING_EXTRACT WITH(NOLOCK)
        //     ) PE ON PE.U_BookingId = BE.U_BookingId
        //     WHERE (
        //         TRY_PARSE(PE.U_GrossClientRatesTax AS FLOAT) <> TRY_PARSE(BE.U_GrossInitialRate AS FLOAT)
        //         OR TRY_PARSE(PE.U_Demurrage AS FLOAT) <> TRY_PARSE(BE.U_Demurrage AS FLOAT)
        //         OR TRY_PARSE(PE.U_TotalAddtlCharges AS FLOAT) <> TRY_PARSE(BE.U_AddCharges AS FLOAT)
        //         OR ((
        //                 (TRY_PARSE(PE.U_GrossClientRatesTax AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_GrossClientRatesTax AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(BE.U_GrossInitialRate AS FLOAT) IS NULL OR TRY_PARSE(BE.U_GrossInitialRate AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_Demurrage AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Demurrage AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(BE.U_Demurrage AS FLOAT) IS NULL OR TRY_PARSE(BE.U_Demurrage AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_TotalAddtlCharges AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_TotalAddtlCharges AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(BE.U_AddCharges AS FLOAT) IS NULL OR TRY_PARSE(BE.U_AddCharges AS FLOAT) = 0)
        //         ))
        //     ) AND PE.U_BookingId IS NOT NULL AND BE.U_BookingId IS NOT NULL
        // `, queryPromiseCallback) as Promise<EventId[]>
        // const tpdiEvent: Promise<EventId[]> = queryPromise(pool, `
        //     SELECT
        //         TE.U_BookingId AS id,
        //         CONCAT('T-BTPDI-', FORMAT(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'}, 'yyyyMMddhhmmss')) AS serial
        //     FROM (
        //         SELECT U_BookingId, U_GrossTruckerRates, U_GrossTruckerRatesN, U_RateBasis, U_Demurrage,
        //         U_AddtlDrop, U_BoomTruck, U_Manpower, U_BackLoad, U_Addtlcharges, U_DemurrageN, U_AddtlChargesN
        //         FROM TP_EXTRACT WITH(NOLOCK)
        //     ) TE
        //     LEFT JOIN (
        //         SELECT U_BookingId, U_GrossTruckerRates, U_GrossTruckerRatesTax, U_RateBasisT, U_Demurrage2,
        //         U_AddtlDrop2, U_BoomTruck2, U_Manpower2, U_Backload2, U_totalAddtlCharges2, U_Demurrage3, U_AddtlCharges
        //         FROM PRICING_EXTRACT WITH(NOLOCK)
        //     ) PE ON PE.U_BookingId = TE.U_BookingId
        //     WHERE (
        //         TRY_PARSE(PE.U_GrossTruckerRates AS FLOAT) <> TRY_PARSE(TE.U_GrossTruckerRates AS FLOAT)
        //         OR TRY_PARSE(PE.U_GrossTruckerRatesTax AS FLOAT) <> TRY_PARSE(TE.U_GrossTruckerRatesN AS FLOAT)
        //         OR TRY_PARSE(PE.U_RateBasisT AS FLOAT) <> TRY_PARSE(TE.U_RateBasis AS FLOAT)
        //         OR TRY_PARSE(PE.U_Demurrage2 AS FLOAT) <> TRY_PARSE(TE.U_Demurrage AS FLOAT)
        //         OR TRY_PARSE(PE.U_AddtlDrop2 AS FLOAT) <> TRY_PARSE(TE.U_AddtlDrop AS FLOAT)
        //         OR TRY_PARSE(PE.U_BoomTruck2 AS FLOAT) <> TRY_PARSE(TE.U_BoomTruck AS FLOAT)
        //         OR TRY_PARSE(PE.U_Manpower2 AS FLOAT) <> TRY_PARSE(TE.U_Manpower AS FLOAT)
        //         OR TRY_PARSE(PE.U_Backload2 AS FLOAT) <> TRY_PARSE(TE.U_BackLoad AS FLOAT)
        //         OR TRY_PARSE(PE.U_totalAddtlCharges2 AS FLOAT) <> TRY_PARSE(TE.U_Addtlcharges AS FLOAT)
        //         OR TRY_PARSE(PE.U_Demurrage3 AS FLOAT) <> TRY_PARSE(TE.U_DemurrageN AS FLOAT)
        //         OR TRY_PARSE(PE.U_AddtlCharges AS FLOAT) <> TRY_PARSE(TE.U_AddtlChargesN AS FLOAT)
        //         OR ((
        //                 (TRY_PARSE(PE.U_GrossTruckerRates AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_GrossTruckerRates AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_GrossTruckerRates AS FLOAT) IS NULL OR TRY_PARSE(TE.U_GrossTruckerRates AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_GrossTruckerRatesTax AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_GrossTruckerRatesTax AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_GrossTruckerRatesN AS FLOAT) IS NULL OR TRY_PARSE(TE.U_GrossTruckerRatesN AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_RateBasisT AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_RateBasisT AS FLOAT) <> '')
        //                 AND (TRY_PARSE(TE.U_RateBasis AS FLOAT) IS NULL OR TRY_PARSE(TE.U_RateBasis AS FLOAT) = '')
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_Demurrage2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Demurrage2 AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_Demurrage AS FLOAT) IS NULL OR TRY_PARSE(TE.U_Demurrage AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_AddtlDrop2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_AddtlDrop2 AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_AddtlDrop AS FLOAT) IS NULL OR TRY_PARSE(TE.U_AddtlDrop AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_BoomTruck2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_BoomTruck2 AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_BoomTruck AS FLOAT) IS NULL OR TRY_PARSE(TE.U_BoomTruck AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_Manpower2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Manpower2 AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_Manpower AS FLOAT) IS NULL OR TRY_PARSE(TE.U_Manpower AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_Backload2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Backload2 AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_BackLoad AS FLOAT) IS NULL OR TRY_PARSE(TE.U_BackLoad AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_totalAddtlCharges2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_totalAddtlCharges2 AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_Addtlcharges AS FLOAT) IS NULL OR TRY_PARSE(TE.U_Addtlcharges AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_Demurrage3 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Demurrage3 AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_DemurrageN AS FLOAT) IS NULL OR TRY_PARSE(TE.U_DemurrageN AS FLOAT) = 0)
        //             )
        //             OR (
        //                 (TRY_PARSE(PE.U_AddtlCharges AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_AddtlCharges AS FLOAT) <> 0)
        //                 AND (TRY_PARSE(TE.U_AddtlChargesN AS FLOAT) IS NULL OR TRY_PARSE(TE.U_AddtlChargesN AS FLOAT) = 0)
        //         ))
        //     ) AND PE.U_BookingId IS NOT NULL AND TE.U_BookingId IS NOT NULL
        // `, queryPromiseCallback) as Promise<EventId[]>
        const ptfEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                POD.U_BookingNumber as id,
                CONCAT('PTF-', FORMAT(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'}, 'yyyyMMddhhmmss')) AS serial
            FROM (
                SELECT U_BookingNumber, U_PTFNo
                FROM [dbo].[@PCTP_POD] WITH(NOLOCK)
            ) POD
            WHERE POD.U_PTFNo IS NOT NULL AND POD.U_PTFNo <> ''
            AND POD.U_BookingNumber IN (
                SELECT U_BookingNumber 
                FROM PCTP_UNIFIED WITH(NOLOCK)
                WHERE U_BookingNumber IS NOT NULL AND (U_PTFNo IS NULL OR U_PTFNo = '')
            ) AND POD.U_BookingNumber IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const missingEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT 
                U_BookingNumber AS id,
                CONCAT('MISSING-', FORMAT(${!!process.env.SIMULATE_DATE ? `CAST('${process.env.SIMULATE_DATE}' AS DATE)` : 'GETDATE()'}, 'yyyyMMddhhmmss')) AS serial
            FROM [@PCTP_POD] WITH(NOLOCK) 
            WHERE U_BookingNumber NOT IN (
                SELECT U_BookingNumber 
                FROM PCTP_UNIFIED WITH(NOLOCK)
            )
        `, queryPromiseCallback) as Promise<EventId[]>
        return new Promise((resolve2, reject2) => {
            Promise.all([
                apEvent, 
                opEvent,
                soEvent, 
                arEvent, 
                bnEvent, 
                dupEvent, 
                // sbdiEvent, 
                // bvnrEvent,
                // tvnrEvent,
                // bpdiEvent,
                // tpdiEvent,
                ptfEvent,
                missingEvent,
            ]).then(async eventIdsArrs => {
                // throw 'Custom Error';
                let tmpFetchedIds: EventId[] = [];
                for (const eventIds of eventIdsArrs) {
                    tmpFetchedIds = [ ...tmpFetchedIds, ...eventIds ]
                }
                const processedIds: string[] = await SAPEventListener.processedIdStorage.get();
                for (const eventId of tmpFetchedIds) {
                    if (!fetchedIds.some(({ id }) => id === eventId.id) 
                        && (!doDisregardAlreadyProcessed 
                            || (!processedIds.includes(`${eventId.id}-${eventId.serial}`) 
                                && !SAPEventListener.newProcessedIds.includes(`${eventId.id}-${eventId.serial}`)))) {
                        fetchedIds.push(eventId);
                    }
                }
                console.log(`fetch elapsed time... ${timer.getElapsedTime()} sec for ${fetchedIds.length} ids`);
                resolve2(!!!process.env.SIMULATE_DATE || !!!process.env.SIMULATE_SIZE ? fetchedIds :
                    ((fetchedIds: EventId[], simulateSize: number): EventId[] => {
                        if (isNaN(simulateSize) || !!!simulateSize) return fetchedIds;
                        const trimmedFetchedIds: EventId[] = [];
                        for (let index = 0; index < simulateSize; index++) {
                            trimmedFetchedIds.push(fetchedIds[index]);
                        }
                        return trimmedFetchedIds;
                    })(fetchedIds, parseInt(process.env.SIMULATE_SIZE || ''))
                );
            }).catch(err => {
                console.log('events error encountered...', err)
                reject2(err);
            })
        }); 
    }

    private async processFetchedIds(livePool: sql.ConnectionPool, fetchedIds: EventId[]) {
        if (SAPEventListener.freshStart && process.env.ENV === 'prod') {
            console.log('SAPEventListener is waiting for 15 seconds, happens only on fresh start...')
            await TimeUtil.timeout(15000);
            SAPEventListener.freshStart = false;
            console.log('current processedIdStorage length: ', (await SAPEventListener.processedIdStorage.get()).length)
        }
        const processedIdsStorage: string[] = process.env.ENV === 'prod' ? await SAPEventListener.processedIdStorage.get() : [];
        const fetchedIdsToProcess: EventId[] = [];
        for (const eventId of fetchedIds as EventId[]) {
            const { id, serial } = eventId;
            if (processedIdsStorage.includes(`${id}-${serial}`)) continue;
            if (fetchedIdsToProcess.some(e => e.id === id)) continue;
            fetchedIdsToProcess.push(eventId);
        }
        if (!!fetchedIdsToProcess) console.log('fetchedIdsToProcess length: ', fetchedIdsToProcess.length)
        let fetchedIdsToProcessLength = fetchedIdsToProcess.length;
        const timer: Timer = new Timer();
        SAPEventListener.newProcessedIds.length = 0
        if (!!fetchedIdsToProcess && fetchedIdsToProcess.length && !!this.numberOfIdsToProcessed && this.numberOfIdsToProcessed>1) {
            for (const eventIds of ArrayUtil.getArrayChunks<EventId>(fetchedIdsToProcess, this.numberOfIdsToProcessed)) {
                timer.reset();
                console.log(`processing ${eventIds.length} ids...`, eventIds, (new Date()).toString())
                if (await this.refreshIds(
                    livePool, 
                    eventIds.map(({ id }) => id)
                )) {
                    console.log(`COMPLETED, storing...`, eventIds, (new Date()).toString());
                    if (process.env.ENV === 'prod') {
                        for (const { id, serial } of eventIds) {
                            processedIdsStorage.push(`${id}-${serial}`);
                            SAPEventListener.newProcessedIds.push(`${id}-${serial}`);
                        }
                    }
                } else {
                    console.log(`FAILED!`, eventIds, (new Date()).toString())
                }
                console.log(`elapsed time... ${timer.getElapsedTime()} sec for ${eventIds.length} ids`);
                fetchedIdsToProcessLength = fetchedIdsToProcessLength - eventIds.length;
                console.log(`remaining... ${Math.abs(fetchedIdsToProcessLength)}`)
            }
        } else {
            let copyFetchedIdsToProcess = [...fetchedIdsToProcess]
            for (const { id, serial } of fetchedIdsToProcess) {
                // prioritizing adhocs
                // if (!!SAPEventListener.newAdhocIds && SAPEventListener.newAdhocIds.)
                SSEController.notifySubscribers(copyFetchedIdsToProcess);
                timer.reset();
                console.log(`processing... ${id}-${serial}`, (new Date()).toString())
                if (await this.refreshIds(livePool, [id])) {
                    console.log(`COMPLETED, storing... ${id}-${serial}`, (new Date()).toString())
                    processedIdsStorage.push(`${id}-${serial}`);
                    SAPEventListener.newProcessedIds.push(`${id}-${serial}`);
                } else {
                    console.log(`FAILED! ${id}-${serial}`, (new Date()).toString())
                }
                copyFetchedIdsToProcess = copyFetchedIdsToProcess.filter(e => e.id !== id);
                console.log(`elapsed time... ${timer.getElapsedTime()} sec`);
                console.log(`remaining... ${--fetchedIdsToProcessLength}`)
            }
            SSEController.notifySubscribers([]);
        }
        if (!!processedIdsStorage) await SAPEventListener.processedIdStorage.set(processedIdsStorage);
        SAPEventListener.newProcessedIds.length = 0
        console.log('current processedIdStorage length: ', processedIdsStorage.length, (new Date()).toString());
    }

    private async refreshIds(pool: sql.ConnectionPool, ids: string[]): Promise<boolean> {
        const quotedIdsCSV: string = ids.map(id => `'${id}'`).reduce((acc, id) => acc+=`${!!acc?',':''}${id}`, '')
        const idsCSV: string = ids.reduce((acc, id) => acc+=`${!!acc?',':''}${id}`, '')
        if (!(await this.executeQuery(pool, `
            UPDATE [@FirstratesTP] 
            SET U_Amount = NULL
            WHERE U_Amount = 'NaN' AND U_BN IN (${quotedIdsCSV});
        `))) return false;
        if (!(await this.executeQuery(pool, `
            UPDATE [@FirstratesTP] 
            SET U_AddlAmount = NULL
            WHERE U_AddlAmount = 'NaN' AND U_BN IN (${quotedIdsCSV});
        `))) return false;
        if (!(await this.executeQuery(pool, `
            DELETE FROM PCTP_UNIFIED WHERE U_BookingNumber IN (${quotedIdsCSV});
        `))) return false;
        if (!(await this.executeQuery(pool, `
            INSERT INTO PCTP_UNIFIED
            SELECT
                su_Code, po_Code, bi_Code, tp_Code, pr_Code, po_DisableTableRow, bi_DisableTableRow, tp_DisableTableRow, bi_DisableSomeFields, tp_DisableSomeFields, pr_DisableSomeFields, pr_DisableSomeFields2, U_BookingDate, 
                U_BookingNumber, bi_U_PODNum, tp_U_PODNum, pr_U_PODNum, U_PODSONum, U_CustomerName, U_GrossClientRates, U_GrossInitialRate, U_Demurrage, tp_U_Demurrage, tp_U_AddtlDrop, pr_U_AddtlDrop, 
                tp_U_BoomTruck, pr_U_BoomTruck, tp_U_BoomTruck2, pr_U_BoomTruck2, U_TPBoomTruck2, tp_U_Manpower, pr_U_Manpower, po_U_BackLoad, tp_U_BackLoad, pr_U_BackLoad, U_TotalAddtlCharges, U_Demurrage2, U_AddtlDrop2, 
                U_Manpower2, U_Backload2, U_totalAddtlCharges2, U_Demurrage3, U_GrossProfit, tp_U_Addtlcharges, pr_U_Addtlcharges, U_DemurrageN, U_AddtlChargesN, U_ActualRates, tp_U_RateAdjustments, bi_U_RateAdjustments, 
                U_TPRateAdjustments, bi_U_ActualDemurrage, tp_U_ActualDemurrage, U_TPActualDemurrage, U_ActualCharges, U_OtherCharges, U_AddCharges, U_ActualBilledRate, U_BillingRateAdjustments, U_BillingActualDemurrage, 
                U_ActualAddCharges, U_GrossClientRatesTax, U_GrossTruckerRates, tp_U_RateBasis, pr_U_RateBasis, U_GrossTruckerRatesN, tp_U_TaxType, pr_U_TaxType, U_GrossTruckerRatesTax, U_RateBasisT, U_TaxTypeT, U_Demurrage4, 
                U_AddtlCharges2, U_GrossProfitC, U_GrossProfitNet, U_TotalInitialClient, U_TotalInitialTruckers, U_TotalGrossProfit, U_ClientTag2, U_ClientName, U_SAPClient, U_ClientTag, U_ClientProject, U_ClientVatStatus, U_TruckerName, 
                U_TruckerSAP, U_TruckerTag, U_TruckerVatStatus, U_TPStatus, U_Aging, U_ISLAND, U_ISLAND_D, U_IFINTERISLAND, U_VERIFICATION_TAT, U_POD_TAT, U_ActualDateRec_Intitial, U_SAPTrucker, U_PlateNumber, U_VehicleTypeCap, 
                U_DeliveryStatus, U_DeliveryDateDTR, U_DeliveryDatePOD, U_NoOfDrops, U_TripType, U_Receivedby, U_ClientReceivedDate, U_InitialHCRecDate, U_ActualHCRecDate, U_DateReturned, U_PODinCharge, U_VerifiedDateHC, U_PTFNo, 
                U_DateForwardedBT, U_BillingDeadline, U_BillingStatus, U_SINo, bi_U_BillingTeam, U_BillingTeam, U_SOBNumber, U_ForwardLoad, U_TypeOfAccessorial, U_TimeInEmptyDem, U_TimeOutEmptyDem, U_VerifiedEmptyDem, 
                U_TimeInLoadedDem, U_TimeOutLoadedDem, U_VerifiedLoadedDem, U_TimeInAdvLoading, U_PenaltiesManual, U_DayOfTheWeek, U_TimeIn, U_TimeOut, U_TotalExceed, U_TotalNoExceed, U_ODOIn, U_ODOOut, U_TotalUsage, 
                U_ClientSubStatus, U_ClientSubOverdue, U_ClientPenaltyCalc, U_PODStatusPayment, U_ProofOfPayment, U_TotalRecClients, U_CheckingTotalBilled, U_Checking, U_CWT2307, U_SOLineNum, U_ARInvLineNum, U_TotalPayable, 
                U_TotalSubPenalty, U_PVNo, U_TPincharge, U_CAandDP, U_Interest, U_OtherDeductions, U_TOTALDEDUCTIONS, U_REMARKS1, U_TotalAR, U_VarAR, U_TotalAP, U_VarTP, U_APInvLineNum, U_PODSubmitDeadline, U_OverdueDays, 
                U_InteluckPenaltyCalc, U_WaivedDays, U_HolidayOrWeekend, U_EWT2307, U_LostPenaltyCalc, U_TotalSubPenalties, U_Waived, U_PercPenaltyCharge, U_Approvedby, U_TotalPenaltyWaived, U_TotalPenalty, U_TotalPayableRec, 
                su_U_APDocNum, pr_U_APDocNum, U_ServiceType, U_InvoiceNo, U_ARDocNum, po_U_DocNum, tp_U_DocNum, U_DocNum, U_Paid, U_ORRefNo, U_ActualPaymentDate, U_PaymentReference, U_PaymentStatus, U_Remarks, 
                tp_U_Remarks, U_GroupProject, U_Attachment, U_DeliveryOrigin, U_Destination, U_OtherPODDoc, U_RemarksPOD, U_PODStatusDetail, U_BTRemarks, U_DestinationClient, U_Remarks2, U_TripTicketNo, U_WaybillNo, U_ShipmentNo, 
                U_ShipmentManifestNo, U_DeliveryReceiptNo, U_SeriesNo, U_OutletNo, U_CBM, U_SI_DRNo, U_DeliveryMode, U_SourceWhse, U_SONo, U_NameCustomer, U_CategoryDR, U_IDNumber, U_ApprovalStatus, U_Status, U_RemarksDTR, 
                U_TotalInvAmount, U_PODDocNum, U_BookingId
            FROM [dbo].fetchGenericPctpDataRows('${idsCSV}') X;
        `))) return false;
        return true;
    }

    public async executeQuery(pool: sql.ConnectionPool, query: string, params?: {name: string, type: sql.ISqlTypeWithLength, value: any}[]): Promise<boolean> {
        return await (async (pool: sql.ConnectionPool): Promise<boolean> => {
            return new Promise(async resolve => {
                if (!!params) {
                    for (const { name, type, value } of params) {
                        const ps: sql.PreparedStatement = new sql.PreparedStatement(pool);
                        await ((ps: sql.PreparedStatement): Promise<boolean> => {
                            return new Promise(resolve => {
                                ps.input(name, type)
                                ps.prepare(query, err => {
                                    if (err) {
                                        console.log(err)
                                        resolve(false);
                                    }
                                    const paramValue: any = {};
                                    paramValue[name] = value;
                                    ps.execute(paramValue, (err, result) => {
                                        if (err) {
                                            console.log(err)
                                            resolve(false);
                                        }
                                        // release the connection after queries are executed
                                        ps.unprepare(err => {
                                            if (err) {
                                                console.log(err)
                                                resolve(false);
                                            }
                                            resolve(true)
                                        })
                                    })
                                })
                            })
                        })(ps)
                    }
                    resolve(true);
                } else {
                    try {
                        pool.query(query, async (err: Error | undefined) => {
                            if (err) {
                                console.log('execute query error', err)
                                resolve(false);
                            }
                            resolve(true);
                        })
                    } catch (error) {
                        console.log('execute query error', error)
                        resolve(false);
                    }
                }
            })
        })(pool)
    }
}