import sql from 'mssql';
import TimeUtil, { Timer } from '../utils/TimeUtil';
import { EventId } from '../types/EventEntity';
import { StorageFactory } from '../factory/StorageFactory';
import { PersistedStorge } from '../factory/storage-interface';
import JSONUtil from '../utils/JSONUtil';
import ArrayUtil from '../utils/ArrayUtil';

export class SAPEventListener {
    public static freshStart: boolean = true;
    public static storageFactory: StorageFactory;
    public static processedIdStorage: PersistedStorge<string>;
    public static livePool: sql.ConnectionPool;
    public static newProcessedIds: String[] = []; //`${id}-${serial}`
    private readonly timeOut: number = 10000;
    private readonly numberOfIdsToProcessed: number = 7;
    private monitoringTime: number = (new Date()).setHours(0,0,0,0);
    constructor(livePool: sql.ConnectionPool) {
        SAPEventListener.livePool = livePool;
        SAPEventListener.storageFactory = new StorageFactory('sapEventListener');
        SAPEventListener.storageFactory.init().then(res => {
            SAPEventListener.storageFactory.factory<string>('processedIds', []).then(storage => {
                SAPEventListener.processedIdStorage = storage;
                console.log(`SAPEventListener.processedIdStorage has been set.`)
            })
        })
    }
    public async run() {
        if (process.env.ENV !== 'prod') {
            while (true) {
                const fetchedIds: EventId[] = [];
                const testEventIds: EventId[] | null = JSONUtil.parseObjectFromFile<EventId[]>('./test_data/event_ids.json');
                if (!!testEventIds) {
                    testEventIds.forEach(eventId => fetchedIds.push(eventId as EventId));
                    const connectionPool: sql.ConnectionPool = await SAPEventListener.livePool.connect();
                    if (!!fetchedIds) await this.processFetchedIds(connectionPool, fetchedIds);
                }
                await TimeUtil.timeout(this.timeOut);
            }
        } else {
            while (true) {
                if (!!SAPEventListener.processedIdStorage && !!SAPEventListener.livePool) {
                    if (this.monitoringTime !== (new Date()).setHours(0,0,0,0)) {
                        this.monitoringTime = (new Date()).setHours(0,0,0,0);
                        SAPEventListener.newProcessedIds.length = 0;
                        await SAPEventListener.processedIdStorage.set([]);
                    }
                    await ((livePool: sql.ConnectionPool, sapEventListener: SAPEventListener): Promise<void> => {
                        let fetchedIds: EventId[] = [];
                        return new Promise(resolve1 => {
                            livePool.connect().then(async function(pool: sql.ConnectionPool) {
                                fetchedIds = await SAPEventListener.getFetchedIdsToRefresh(pool);
                                if (!!fetchedIds) await sapEventListener.processFetchedIds(pool, fetchedIds);
                                resolve1()
                            }).catch(function (err) {
                                console.error('Error creating connection pool', err)
                            });
                        })
                    })(SAPEventListener.livePool, this);
                    await TimeUtil.timeout(this.timeOut)
                } else {
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
            WHERE CAST(CreateDate AS date) = CAST(GETDATE() AS date)
            OR CAST(UpdateDate AS date) = CAST(GETDATE() AS date)
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
        const soEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                line.ItemCode AS id,
                CONCAT('SO', head.DocNum, '-', FORMAT(head.UpdateDate, 'yyyyMMdd'), head.UpdateTS) AS serial
            FROM (
            SELECT DocEntry, DocNum, UpdateDate, UpdateTS
            FROM ORDR WITH(NOLOCK)
            WHERE CAST(CreateDate AS date) = CAST(GETDATE() AS date)
            OR CAST(UpdateDate AS date) = CAST(GETDATE() AS date)
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
            WHERE CAST(CreateDate AS date) = CAST(GETDATE() AS date)
            OR CAST(UpdateDate AS date) = CAST(GETDATE() AS date)
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
            AND CAST(head.CreateDate AS date) = CAST(GETDATE() AS date)
            AND (
                EXISTS(SELECT 1 FROM [@PCTP_POD] WITH(NOLOCK) WHERE U_BookingNumber = head.ItemCode)
                OR EXISTS(SELECT 1 FROM [@PCTP_PRICING] WITH(NOLOCK) WHERE U_BookingId = head.ItemCode)
            )
            AND (
                NOT EXISTS(SELECT 1 FROM SUMMARY_EXTRACT WITH(NOLOCK) WHERE U_BookingNumber = head.ItemCode)
                OR NOT EXISTS(SELECT 1 FROM POD_EXTRACT WITH(NOLOCK) WHERE U_BookingNumber = head.ItemCode)
                OR NOT EXISTS(SELECT 1 FROM BILLING_EXTRACT WITH(NOLOCK) WHERE U_BookingId = head.ItemCode)
                OR NOT EXISTS(SELECT 1 FROM TP_EXTRACT WITH(NOLOCK) WHERE U_BookingId = head.ItemCode)
                OR NOT EXISTS(SELECT 1 FROM PRICING_EXTRACT WITH(NOLOCK) WHERE U_BookingId = head.ItemCode)
            )
        `, queryPromiseCallback) as Promise<EventId[]>
        const dupEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT Z.id, Z.serial FROM (
                SELECT 
                    U_BookingNumber AS id,
                    CONCAT('DUP-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
                FROM SUMMARY_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                UNION
                SELECT 
                    U_BookingNumber AS id,
                    CONCAT('DUP-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
                FROM POD_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                UNION
                SELECT 
                    U_BookingNumber AS id,
                    CONCAT('DUP-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
                FROM BILLING_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                UNION
                SELECT 
                    U_BookingNumber AS id,
                    CONCAT('DUP-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
                FROM TP_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                UNION
                SELECT 
                    U_BookingNumber AS id,
                    CONCAT('DUP-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
                FROM PRICING_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
            ) Z
            LEFT JOIN
            (
                SELECT X.id,
                CONCAT(CASE
                        WHEN (SELECT COUNT(*) FROM [@PCTP_POD] WITH(NOLOCK) WHERE U_BookingNumber = X.id) > 1 THEN 'POD '
                        ELSE ''
                    END,
                    CASE
                        WHEN (SELECT COUNT(*) FROM [@PCTP_BILLING] WITH(NOLOCK) WHERE U_BookingId = X.id) > 1 THEN 'BILLING '
                        ELSE ''
                    END,
                    CASE
                        WHEN (SELECT COUNT(*) FROM [@PCTP_TP] WITH(NOLOCK) WHERE U_BookingId = X.id) > 1 THEN 'TP '
                        ELSE ''
                    END,
                    CASE
                        WHEN (SELECT COUNT(*) FROM [@PCTP_PRICING] WITH(NOLOCK) WHERE U_BookingId = X.id) > 1 THEN 'PRICING '
                        ELSE ''
                    END) AS DuplicateInMainTable
                FROM (SELECT 
                        U_BookingNumber as id
                    FROM SUMMARY_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                    UNION
                    SELECT 
                        U_BookingNumber as id
                    FROM POD_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                    UNION
                    SELECT 
                        U_BookingNumber as id
                    FROM BILLING_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                    UNION
                    SELECT 
                        U_BookingNumber as id
                    FROM TP_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                    UNION
                    SELECT 
                        U_BookingNumber as id
                    FROM PRICING_EXTRACT WITH(NOLOCK) GROUP BY U_BookingNumber HAVING COUNT(*) > 1
                ) X
                WHERE X.id IS NOT NULL
            ) Y ON Z.id = Y.id
            WHERE Y.id IS NOT NULL 
            AND (
                Y.DuplicateInMainTable = '' 
                OR Y.DuplicateInMainTable IS NULL
            )
        `, queryPromiseCallback) as Promise<EventId[]>
        const sbdiEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                SE.U_BookingNumber as id,
                CONCAT('SBDI-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
            FROM (
                SELECT U_BookingNumber, U_BillingStatus, U_InvoiceNo, U_PODSONum, U_ARDocNum
                FROM SUMMARY_EXTRACT WITH(NOLOCK)
            ) SE
            LEFT JOIN (
                SELECT U_BookingId, U_BillingStatus, U_InvoiceNo, U_PODSONum, U_DocNum
                FROM BILLING_EXTRACT WITH(NOLOCK)
            ) BE ON BE.U_BookingId = SE.U_BookingNumber
            WHERE ((
                (BE.U_BillingStatus <> SE.U_BillingStatus AND REPLACE(BE.U_BillingStatus, ' ', '') <> REPLACE(SE.U_BillingStatus, ' ', '')) 
                OR (
                    (
                        BE.U_BillingStatus IS NOT NULL AND REPLACE(BE.U_BillingStatus, ' ', '') <> '' 
                        AND (SE.U_BillingStatus IS NULL OR REPLACE(SE.U_BillingStatus, ' ', '') = '')
                    )
                    OR (
                        SE.U_BillingStatus IS NOT NULL AND REPLACE(SE.U_BillingStatus, ' ', '') <> '' 
                        AND (BE.U_BillingStatus IS NULL OR REPLACE(BE.U_BillingStatus, ' ', '') = '')
                    )
                )
            )
            OR (
                (BE.U_InvoiceNo <> SE.U_InvoiceNo AND REPLACE(BE.U_InvoiceNo, ' ', '') <> REPLACE(SE.U_InvoiceNo, ' ', '')) 
                OR (
                    (
                        BE.U_InvoiceNo IS NOT NULL AND REPLACE(BE.U_InvoiceNo, ' ', '') <> '' 
                        AND (SE.U_InvoiceNo IS NULL OR REPLACE(SE.U_InvoiceNo, ' ', '') = '')
                    )
                    OR (
                        SE.U_InvoiceNo IS NOT NULL AND REPLACE(SE.U_InvoiceNo, ' ', '') <> '' 
                        AND (BE.U_InvoiceNo IS NULL OR REPLACE(BE.U_InvoiceNo, ' ', '') = '')
                    )
                )
            )
            OR (
                (BE.U_PODSONum <> SE.U_PODSONum AND REPLACE(BE.U_PODSONum, ' ', '') <> REPLACE(SE.U_PODSONum, ' ', '')) 
                OR (
                    (
                        BE.U_PODSONum IS NOT NULL AND REPLACE(BE.U_PODSONum, ' ', '') <> '' 
                        AND (SE.U_PODSONum IS NULL OR REPLACE(SE.U_PODSONum, ' ', '') = '')
                    )
                    OR (
                        SE.U_PODSONum IS NOT NULL AND REPLACE(SE.U_PODSONum, ' ', '') <> '' 
                        AND (BE.U_PODSONum IS NULL OR REPLACE(BE.U_PODSONum, ' ', '') = '')
                    )
                )
            )
            OR (
                (BE.U_DocNum <> SE.U_ARDocNum AND REPLACE(BE.U_DocNum, ' ', '') <> REPLACE(SE.U_ARDocNum, ' ', '')) 
                OR (
                    (
                        BE.U_DocNum IS NOT NULL AND REPLACE(BE.U_DocNum, ' ', '') <> '' 
                        AND (SE.U_ARDocNum IS NULL OR REPLACE(SE.U_ARDocNum, ' ', '') = '')
                    )
                    OR (
                        SE.U_ARDocNum IS NOT NULL AND REPLACE(SE.U_ARDocNum, ' ', '') <> '' 
                        AND (BE.U_DocNum IS NULL OR REPLACE(BE.U_DocNum, ' ', '') = '')
                    )
                )
            ))
            AND BE.U_BookingId IS NOT NULL AND SE.U_BookingNumber IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const bvnrEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                T0.U_BookingNumber as id,
                CONCAT('B-TBVNR-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
            FROM (
                SELECT U_BookingNumber, U_PODStatusDetail
                FROM [dbo].[@PCTP_POD] WITH(NOLOCK)
            ) T0
            WHERE 1=1
            AND (CAST(T0.U_PODStatusDetail as nvarchar(100)) LIKE '%Verified%' OR CAST(T0.U_PODStatusDetail as nvarchar(100)) LIKE '%ForAdvanceBilling%')
            AND T0.U_BookingNumber NOT IN (SELECT U_BookingId FROM BILLING_EXTRACT WITH(NOLOCK))
            AND T0.U_BookingNumber IN (SELECT U_BookingId FROM [dbo].[@PCTP_BILLING] WITH(NOLOCK) WHERE U_BookingId IS NOT NULL)
            AND T0.U_BookingNumber IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const tvnrEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                T0.U_BookingNumber as id,
                CONCAT('T-TBVNR-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
            FROM [dbo].[@PCTP_POD] T0 WITH(NOLOCK)
            WHERE 1=1
            AND (CAST(T0.U_PODStatusDetail as nvarchar(100)) LIKE '%Verified%')
            AND T0.U_BookingNumber NOT IN (SELECT U_BookingId FROM TP_EXTRACT WITH(NOLOCK) WHERE U_BookingId IS NOT NULL)
            AND T0.U_BookingNumber IN (SELECT U_BookingId FROM [dbo].[@PCTP_TP] WITH(NOLOCK) WHERE U_BookingId IS NOT NULL)
            AND T0.U_BookingNumber IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const bpdiEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                BE.U_BookingId AS id,
                CONCAT('B-BTPDI-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
            FROM (
                SELECT U_BookingId, U_GrossInitialRate, U_Demurrage, U_AddCharges
                FROM BILLING_EXTRACT WITH(NOLOCK)
            ) BE
            LEFT JOIN (
                SELECT U_BookingId, U_GrossClientRatesTax, U_Demurrage, U_TotalAddtlCharges
                FROM PRICING_EXTRACT WITH(NOLOCK)
            ) PE ON PE.U_BookingId = BE.U_BookingId
            WHERE (
                TRY_PARSE(PE.U_GrossClientRatesTax AS FLOAT) <> TRY_PARSE(BE.U_GrossInitialRate AS FLOAT)
                OR TRY_PARSE(PE.U_Demurrage AS FLOAT) <> TRY_PARSE(BE.U_Demurrage AS FLOAT)
                OR TRY_PARSE(PE.U_TotalAddtlCharges AS FLOAT) <> TRY_PARSE(BE.U_AddCharges AS FLOAT)
                OR ((
                        (TRY_PARSE(PE.U_GrossClientRatesTax AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_GrossClientRatesTax AS FLOAT) <> 0)
                        AND (TRY_PARSE(BE.U_GrossInitialRate AS FLOAT) IS NULL OR TRY_PARSE(BE.U_GrossInitialRate AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_Demurrage AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Demurrage AS FLOAT) <> 0)
                        AND (TRY_PARSE(BE.U_Demurrage AS FLOAT) IS NULL OR TRY_PARSE(BE.U_Demurrage AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_TotalAddtlCharges AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_TotalAddtlCharges AS FLOAT) <> 0)
                        AND (TRY_PARSE(BE.U_AddCharges AS FLOAT) IS NULL OR TRY_PARSE(BE.U_AddCharges AS FLOAT) = 0)
                ))
            ) AND PE.U_BookingId IS NOT NULL AND BE.U_BookingId IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const tpdiEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                TE.U_BookingId AS id,
                CONCAT('T-BTPDI-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
            FROM (
                SELECT U_BookingId, U_GrossTruckerRates, U_GrossTruckerRatesN, U_RateBasis, U_Demurrage,
                U_AddtlDrop, U_BoomTruck, U_Manpower, U_BackLoad, U_Addtlcharges, U_DemurrageN, U_AddtlChargesN
                FROM TP_EXTRACT WITH(NOLOCK)
            ) TE
            LEFT JOIN (
                SELECT U_BookingId, U_GrossTruckerRates, U_GrossTruckerRatesTax, U_RateBasisT, U_Demurrage2,
                U_AddtlDrop2, U_BoomTruck2, U_Manpower2, U_Backload2, U_totalAddtlCharges2, U_Demurrage3, U_AddtlCharges
                FROM PRICING_EXTRACT WITH(NOLOCK)
            ) PE ON PE.U_BookingId = TE.U_BookingId
            WHERE (
                TRY_PARSE(PE.U_GrossTruckerRates AS FLOAT) <> TRY_PARSE(TE.U_GrossTruckerRates AS FLOAT)
                OR TRY_PARSE(PE.U_GrossTruckerRatesTax AS FLOAT) <> TRY_PARSE(TE.U_GrossTruckerRatesN AS FLOAT)
                OR TRY_PARSE(PE.U_RateBasisT AS FLOAT) <> TRY_PARSE(TE.U_RateBasis AS FLOAT)
                OR TRY_PARSE(PE.U_Demurrage2 AS FLOAT) <> TRY_PARSE(TE.U_Demurrage AS FLOAT)
                OR TRY_PARSE(PE.U_AddtlDrop2 AS FLOAT) <> TRY_PARSE(TE.U_AddtlDrop AS FLOAT)
                OR TRY_PARSE(PE.U_BoomTruck2 AS FLOAT) <> TRY_PARSE(TE.U_BoomTruck AS FLOAT)
                OR TRY_PARSE(PE.U_Manpower2 AS FLOAT) <> TRY_PARSE(TE.U_Manpower AS FLOAT)
                OR TRY_PARSE(PE.U_Backload2 AS FLOAT) <> TRY_PARSE(TE.U_BackLoad AS FLOAT)
                OR TRY_PARSE(PE.U_totalAddtlCharges2 AS FLOAT) <> TRY_PARSE(TE.U_Addtlcharges AS FLOAT)
                OR TRY_PARSE(PE.U_Demurrage3 AS FLOAT) <> TRY_PARSE(TE.U_DemurrageN AS FLOAT)
                OR TRY_PARSE(PE.U_AddtlCharges AS FLOAT) <> TRY_PARSE(TE.U_AddtlChargesN AS FLOAT)
                OR ((
                        (TRY_PARSE(PE.U_GrossTruckerRates AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_GrossTruckerRates AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_GrossTruckerRates AS FLOAT) IS NULL OR TRY_PARSE(TE.U_GrossTruckerRates AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_GrossTruckerRatesTax AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_GrossTruckerRatesTax AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_GrossTruckerRatesN AS FLOAT) IS NULL OR TRY_PARSE(TE.U_GrossTruckerRatesN AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_RateBasisT AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_RateBasisT AS FLOAT) <> '')
                        AND (TRY_PARSE(TE.U_RateBasis AS FLOAT) IS NULL OR TRY_PARSE(TE.U_RateBasis AS FLOAT) = '')
                    )
                    OR (
                        (TRY_PARSE(PE.U_Demurrage2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Demurrage2 AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_Demurrage AS FLOAT) IS NULL OR TRY_PARSE(TE.U_Demurrage AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_AddtlDrop2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_AddtlDrop2 AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_AddtlDrop AS FLOAT) IS NULL OR TRY_PARSE(TE.U_AddtlDrop AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_BoomTruck2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_BoomTruck2 AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_BoomTruck AS FLOAT) IS NULL OR TRY_PARSE(TE.U_BoomTruck AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_Manpower2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Manpower2 AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_Manpower AS FLOAT) IS NULL OR TRY_PARSE(TE.U_Manpower AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_Backload2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Backload2 AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_BackLoad AS FLOAT) IS NULL OR TRY_PARSE(TE.U_BackLoad AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_totalAddtlCharges2 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_totalAddtlCharges2 AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_Addtlcharges AS FLOAT) IS NULL OR TRY_PARSE(TE.U_Addtlcharges AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_Demurrage3 AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_Demurrage3 AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_DemurrageN AS FLOAT) IS NULL OR TRY_PARSE(TE.U_DemurrageN AS FLOAT) = 0)
                    )
                    OR (
                        (TRY_PARSE(PE.U_AddtlCharges AS FLOAT) IS NOT NULL AND TRY_PARSE(PE.U_AddtlCharges AS FLOAT) <> 0)
                        AND (TRY_PARSE(TE.U_AddtlChargesN AS FLOAT) IS NULL OR TRY_PARSE(TE.U_AddtlChargesN AS FLOAT) = 0)
                ))
            ) AND PE.U_BookingId IS NOT NULL AND TE.U_BookingId IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        const ptfEvent: Promise<EventId[]> = queryPromise(pool, `
            SELECT
                POD.U_BookingNumber as id,
                CONCAT('PTF-', FORMAT(GETDATE(), 'yyyyMMddhhmmss')) AS serial
            FROM (
                SELECT U_BookingNumber, U_PTFNo
                FROM [dbo].[@PCTP_POD] WITH(NOLOCK)
            ) POD
            WHERE POD.U_PTFNo IS NOT NULL AND POD.U_PTFNo <> ''
            AND POD.U_BookingNumber IN (
                SELECT U_BookingNumber 
                FROM POD_EXTRACT WITH(NOLOCK)
                WHERE U_BookingNumber IS NOT NULL AND (U_PTFNo IS NULL OR U_PTFNo = '')
            ) AND POD.U_BookingNumber IS NOT NULL
        `, queryPromiseCallback) as Promise<EventId[]>
        return new Promise(resolve2 => {
            Promise.all([
                apEvent, 
                soEvent, 
                arEvent, 
                bnEvent, 
                dupEvent, 
                sbdiEvent, 
                bvnrEvent,
                tvnrEvent,
                bpdiEvent,
                tpdiEvent,
                ptfEvent
            ]).then(async eventIdsArrs => {
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
                resolve2(fetchedIds);
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
            for (const { id, serial } of fetchedIdsToProcess) {
                // const paramObj: {name: string, type: sql.ISqlTypeWithLength, value: any}[] = [{ name: 'id', type: sql.VarChar(50), value: id }];
                timer.reset();
                console.log(`processing... ${id}-${serial}`, (new Date()).toString())
                if (await this.refreshIds(livePool, [id])) {
                    console.log(`COMPLETED, storing... ${id}-${serial}`, (new Date()).toString())
                    processedIdsStorage.push(`${id}-${serial}`);
                    SAPEventListener.newProcessedIds.push(`${id}-${serial}`);
                } else {
                    console.log(`FAILED! ${id}-${serial}`, (new Date()).toString())
                }
                console.log(`elapsed time... ${timer.getElapsedTime()} sec`);
                console.log(`remaining... ${--fetchedIdsToProcessLength}`)
            }
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
            -----> SUMMARY
            DELETE FROM SUMMARY_EXTRACT WHERE U_BookingNumber IN (${quotedIdsCSV});
        `))) return false;
        if (!(await this.executeQuery(pool, `
            INSERT INTO SUMMARY_EXTRACT
            SELECT
                X.Code, X.U_BookingNumber, X.U_BookingDate, X.U_ClientName, X.U_SAPClient, X.U_ClientVatStatus, X.U_TruckerName, X.U_SAPTrucker, X.U_TruckerVatStatus, X.U_VehicleTypeCap, X.U_ISLAND, X.U_ISLAND_D, X.U_IFINTERISLAND, X.U_DeliveryStatus, X.U_DeliveryDateDTR,
                X.U_DeliveryDatePOD, X.U_ClientReceivedDate, X.U_ActualDateRec_Intitial, X.U_InitialHCRecDate, X.U_ActualHCRecDate, X.U_DateReturned, X.U_VerifiedDateHC, X.U_PTFNo, X.U_DateForwardedBT, X.U_PODSONum, X.U_GrossClientRates,
                X.U_GrossClientRatesTax, X.U_GrossTruckerRates, X.U_GrossTruckerRatesTax, X.U_GrossProfitNet, X.U_TotalInitialClient, X.U_TotalInitialTruckers, X.U_TotalGrossProfit, X.U_BillingStatus, X.U_PODStatusPayment, X.U_PaymentReference,
                X.U_PaymentStatus, X.U_ProofOfPayment, X.U_TotalRecClients, X.U_TotalPayable, X.U_PVNo, X.U_TotalAR, X.U_VarAR, X.U_TotalAP, X.U_VarTP, X.U_APDocNum, X.U_ARDocNum, X.U_DeliveryOrigin, X.U_Destination, X.U_PODStatusDetail, X.U_Remarks, X.U_WaybillNo, X.U_ServiceType,
                X.U_InvoiceNo
            FROM [dbo].fetchPctpDataRows('SUMMARY', '${idsCSV}', DEFAULT) X;
        `))) return false;
        if (!(await this.executeQuery(pool, `
            -----> POD
            DELETE FROM POD_EXTRACT WHERE U_BookingNumber IN (${quotedIdsCSV});
        `))) return false;
        if (!(await this.executeQuery(pool, `
            INSERT INTO POD_EXTRACT
            SELECT
                X.DisableTableRow, X.Code, X.U_BookingDate, X.U_BookingNumber, X.U_PODSONum, X.U_ClientName, X.U_SAPClient, X.U_TruckerName, X.U_ISLAND, X.U_ISLAND_D, 
                X.U_IFINTERISLAND, X.U_VERIFICATION_TAT, X.U_POD_TAT, X.U_ActualDateRec_Intitial, X.U_SAPTrucker, X.U_PlateNumber, X.U_VehicleTypeCap, 
                X.U_DeliveryStatus, X.U_DeliveryDateDTR, X.U_DeliveryDatePOD, X.U_NoOfDrops, X.U_TripType, X.U_Receivedby, X.U_ClientReceivedDate, 
                X.U_InitialHCRecDate, X.U_ActualHCRecDate, X.U_DateReturned, X.U_PODinCharge, X.U_VerifiedDateHC, X.U_PTFNo, X.U_DateForwardedBT, X.U_BillingDeadline, 
                X.U_BillingStatus, X.U_ServiceType, X.U_SINo, X.U_BillingTeam, X.U_SOBNumber, X.U_ForwardLoad, X.U_BackLoad, X.U_TypeOfAccessorial, X.U_TimeInEmptyDem, 
                X.U_TimeOutEmptyDem, X.U_VerifiedEmptyDem, X.U_TimeInLoadedDem, X.U_TimeOutLoadedDem, X.U_VerifiedLoadedDem, X.U_TimeInAdvLoading, X.U_PenaltiesManual, 
                X.U_DayOfTheWeek, X.U_TimeIn, X.U_TimeOut, X.U_TotalNoExceed, X.U_ODOIn, X.U_ODOOut, X.U_TotalUsage, X.U_ClientSubStatus, X.U_ClientSubOverdue, 
                X.U_ClientPenaltyCalc, X.U_PODStatusPayment, X.U_PODSubmitDeadline, X.U_OverdueDays, X.U_InteluckPenaltyCalc, X.U_WaivedDays, X.U_HolidayOrWeekend, 
                X.U_LostPenaltyCalc, X.U_TotalSubPenalties, X.U_Waived, X.U_PercPenaltyCharge, X.U_Approvedby, X.U_TotalPenaltyWaived, X.U_GroupProject, X.U_Attachment, X.U_DeliveryOrigin, X.U_Destination, X.U_Remarks, X.U_OtherPODDoc, X.U_RemarksPOD, 
                X.U_PODStatusDetail, X.U_BTRemarks, X.U_DestinationClient, X.U_Remarks2, X.U_DocNum, X.U_TripTicketNo, X.U_WaybillNo, X.U_ShipmentNo, X.U_DeliveryReceiptNo, 
                X.U_SeriesNo, X.U_OutletNo, X.U_CBM, X.U_SI_DRNo, X.U_DeliveryMode, X.U_SourceWhse, X.U_SONo, X.U_NameCustomer, X.U_CategoryDR, X.U_IDNumber, X.U_ApprovalStatus, 
                X.U_TotalInvAmount
            FROM [dbo].fetchPctpDataRows('POD', '${idsCSV}', DEFAULT) X;
        `))) return false;
        if (!(await this.executeQuery(pool, `
            -----> BILLING
            DELETE FROM BILLING_EXTRACT WHERE U_BookingNumber IN (${quotedIdsCSV});
        `))) return false;
        if (!(await this.executeQuery(pool, `
            INSERT INTO BILLING_EXTRACT
            SELECT
                X.U_BookingNumber, X.DisableTableRow, X.DisableSomeFields, X.Code, X.U_BookingId, X.U_BookingDate, X.U_PODNum, X.U_PODSONum, X.U_CustomerName, X.U_SAPClient, X.U_PlateNumber, X.U_VehicleTypeCap, X.U_DeliveryStatus, X.U_DeliveryDatePOD,
                X.U_NoOfDrops, X.U_TripType, X.U_ClientReceivedDate, X.U_ActualHCRecDate, X.U_PODinCharge, X.U_VerifiedDateHC, X.U_PTFNo, X.U_DateForwardedBT, X.U_BillingDeadline, X.U_BillingStatus, X.U_BillingTeam, X.U_GrossInitialRate, X.U_Demurrage,
                X.U_AddCharges, X.U_ActualBilledRate, X.U_RateAdjustments, X.U_ActualDemurrage, X.U_ActualAddCharges, X.U_TotalRecClients, X.U_CheckingTotalBilled, X.U_Checking, X.U_CWT2307, X.U_SOBNumber, X.U_ForwardLoad, X.U_BackLoad,
                X.U_TypeOfAccessorial, X.U_TimeInEmptyDem, X.U_TimeOutEmptyDem, X.U_VerifiedEmptyDem, X.U_TimeInLoadedDem, X.U_TimeOutLoadedDem, X.U_VerifiedLoadedDem, X.U_TimeInAdvLoading, X.U_DayOfTheWeek, X.U_TimeIn, X.U_TimeOut,
                X.U_TotalExceed, X.U_ODOIn, X.U_ODOOut, X.U_TotalUsage, X.U_SOLineNum, X.U_ARInvLineNum, X.U_TotalAR, X.U_VarAR, X.U_ServiceType, X.U_DocNum, X.U_InvoiceNo, X.U_DeliveryReceiptNo, X.U_SeriesNo, X.U_GroupProject, X.U_DeliveryOrigin,
                X.U_Destination, X.U_OtherPODDoc, X.U_RemarksPOD, X.U_PODStatusDetail, X.U_BTRemarks, X.U_DestinationClient, X.U_Remarks, X.U_Attachment, X.U_SI_DRNo, X.U_TripTicketNo, X.U_WaybillNo, X.U_ShipmentManifestNo, X.U_OutletNo, X.U_CBM,
                X.U_DeliveryMode, X.U_SourceWhse, X.U_SONo, X.U_NameCustomer, X.U_CategoryDR, X.U_IDNumber, X.U_Status, X.U_TotalInvAmount
            FROM [dbo].fetchPctpDataRows('BILLING', '${idsCSV}', DEFAULT) X;
        `))) return false;
        if (!(await this.executeQuery(pool, `
            -----> TP
            DELETE FROM TP_EXTRACT WHERE U_BookingNumber IN (${quotedIdsCSV});
        `))) return false;
        if (!(await this.executeQuery(pool, `
            INSERT INTO TP_EXTRACT
            SELECT
                '' AS WaivedDaysx, '' AS xHolidayOrWeekend,
                X.DisableTableRow, X.U_BookingNumber, X.DisableSomeFields, X.Code, X.U_BookingId, X.U_BookingDate, X.U_PODNum, X.U_PODSONum, X.U_ClientName, X.U_TruckerName, X.U_TruckerSAP, X.U_PlateNumber, X.U_VehicleTypeCap, X.U_ISLAND, X.U_ISLAND_D, 
                X.U_IFINTERISLAND, X.U_DeliveryStatus, X.U_DeliveryDatePOD, X.U_NoOfDrops, X.U_TripType, X.U_Receivedby, X.U_ClientReceivedDate, X.U_ActualDateRec_Intitial, X.U_ActualHCRecDate, X.U_DateReturned, X.U_PODinCharge, X.U_VerifiedDateHC, 
                X.U_TPStatus, X.U_Aging, X.U_GrossTruckerRates, X.U_RateBasis, X.U_GrossTruckerRatesN, X.U_TaxType, X.U_Demurrage, X.U_AddtlDrop, X.U_BoomTruck, X.U_BoomTruck2, X.U_Manpower, X.U_BackLoad, X.U_Addtlcharges, X.U_DemurrageN, 
                X.U_AddtlChargesN, X.U_ActualRates, X.U_RateAdjustments, X.U_ActualDemurrage, X.U_ActualCharges, X.U_OtherCharges, X.U_ClientSubOverdue, X.U_ClientPenaltyCalc, X.U_InteluckPenaltyCalc, 
                X.U_InitialHCRecDate, X.U_DeliveryDateDTR, X.U_TotalInitialTruckers, X.U_LostPenaltyCalc, X.U_TotalSubPenalty, X.U_TotalPenaltyWaived, X.U_TotalPenalty, X.U_TotalPayable, X.U_EWT2307, X.U_TotalPayableRec, X.U_PVNo, X.U_ORRefNo, X.U_TPincharge, 
                X.U_CAandDP, X.U_Interest, X.U_OtherDeductions, X.U_TOTALDEDUCTIONS, X.U_REMARKS1, X.U_TotalAP, X.U_VarTP, X.U_APInvLineNum, X.U_PercPenaltyCharge, X.U_DocNum, X.U_Paid, X.U_OtherPODDoc, X.U_DeliveryOrigin, X.U_Remarks2, 
                X.U_RemarksPOD, X.U_GroupProject, X.U_Destination, X.U_Remarks, X.U_Attachment, X.U_TripTicketNo, X.U_WaybillNo, X.U_ShipmentManifestNo, X.U_DeliveryReceiptNo, X.U_SeriesNo, X.U_ActualPaymentDate, X.U_PaymentReference, 
                X.U_PaymentStatus
            FROM [dbo].fetchPctpDataRows('TP', '${idsCSV}', DEFAULT) X;
        `))) return false;
        if (!(await this.executeQuery(pool, `
            -----> PRICING
            DELETE FROM PRICING_EXTRACT WHERE U_BookingNumber IN (${quotedIdsCSV});
        `))) return false;
        if (!(await this.executeQuery(pool, `
            INSERT INTO PRICING_EXTRACT
            SELECT
                X.U_BookingNumber, X.DisableSomeFields, X.DisableSomeFields2, X.Code, X.U_BookingId, X.U_BookingDate, X.U_PODNum, X.U_CustomerName, X.U_ClientTag, X.U_ClientProject, X.U_TruckerName, X.U_TruckerTag, X.U_VehicleTypeCap, X.U_DeliveryStatus,
                X.U_TripType, X.U_NoOfDrops, X.U_GrossClientRates, X.U_ISLAND, X.U_ISLAND_D, X.U_IFINTERISLAND, X.U_GrossClientRatesTax, X.U_RateBasis, X.U_TaxType, X.U_GrossProfitNet, X.U_Demurrage, X.U_AddtlDrop, X.U_BoomTruck, X.U_Manpower, X.U_Backload,
                X.U_TotalAddtlCharges, X.U_Demurrage2, X.U_AddtlDrop2, X.U_BoomTruck2, X.U_Manpower2, X.U_Backload2, X.U_totalAddtlCharges2, X.U_Demurrage3, X.U_AddtlCharges, X.U_GrossProfit, X.U_TotalInitialClient, X.U_TotalInitialTruckers, X.U_TotalGrossProfit,
                X.U_ClientTag2, X.U_GrossTruckerRates, X.U_GrossTruckerRatesTax, X.U_RateBasisT, X.U_TaxTypeT, X.U_Demurrage4, X.U_AddtlCharges2, X.U_GrossProfitC, X.U_ActualBilledRate, X.U_BillingRateAdjustments,
                X.U_BillingActualDemurrage, X.U_ActualAddCharges, X.U_TotalRecClients, X.U_TotalAR, X.U_VarAR, X.U_PODSONum, X.U_ActualRates, X.U_TPRateAdjustments, X.U_TPActualDemurrage, X.U_ActualCharges, X.U_TPBoomTruck2, X.U_OtherCharges,
                X.U_TotalPayable, X.U_PVNo, X.U_TotalAP, X.U_VarTP, X.U_APDocNum, X.U_Paid, X.U_DocNum, X.U_DeliveryOrigin, X.U_Destination, X.U_RemarksDTR, X.U_RemarksPOD, X.U_PODDocNum
            FROM [dbo].fetchPctpDataRows('PRICING', '${idsCSV}', DEFAULT) X;
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
                    pool.query(query, async (err: Error | undefined) => {
                        if (err) {
                            console.log('execute query error', err)
                            resolve(false);
                        }
                        resolve(true);
                    });
                }
            })
        })(pool)
    }
}