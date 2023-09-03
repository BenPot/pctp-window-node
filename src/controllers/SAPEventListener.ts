import sql from 'mssql';
import TimeUtil from '../utils/TimeUtil';
import { EventId } from '../types/EventEntity';

export class SAPEventListener {
    public static livePool: sql.ConnectionPool;
    public static processedIds: String[] = []; //`${id}-${serial}`
    private readonly timeOut: number = 10000;
    private monitoringTime: number = (new Date()).setHours(0,0,0,0);
    constructor(livePool: sql.ConnectionPool) {
        SAPEventListener.livePool = livePool;
    }
    public async run() {
        while (true) {
            if (this.monitoringTime !== (new Date()).setHours(0,0,0,0)) {
                this.monitoringTime = (new Date()).setHours(0,0,0,0);
                SAPEventListener.processedIds.length = 0;
            }
            await ((livePool: sql.ConnectionPool, sapEventListener: SAPEventListener): Promise<void> => {
                const fetchedIds: EventId[] = [];
                return new Promise(resolve => {
                    livePool.connect().then(async function(pool) {
                        await ((pool: sql.ConnectionPool): Promise<void> => {
                            return new Promise(resolve => {
                                pool.query(`
                                    SELECT
                                        TP.U_BookingNumber AS id,
                                        CONCAT('AP-', FORMAT(head.UpdateDate, 'yyyyMMdd'), head.UpdateTS) AS serial
                                    FROM (
                                    SELECT DocEntry, U_PVNo, UpdateDate, UpdateTS
                                    FROM OPCH WITH(NOLOCK)
                                    WHERE CAST(CreateDate AS date) = CAST(GETDATE() AS date)
                                    OR CAST(UpdateDate AS date) = CAST(GETDATE() AS date)
                                    ) head
                                    LEFT JOIN PCH1 line ON head.DocEntry = line.DocEntry
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
                                    
                                    UNION
                                    
                                    SELECT
                                        line.ItemCode AS id,
                                        CONCAT('SO-', FORMAT(head.UpdateDate, 'yyyyMMdd'), head.UpdateTS) AS serial
                                    FROM (
                                    SELECT DocEntry, UpdateDate, UpdateTS
                                    FROM ORDR WITH(NOLOCK)
                                    WHERE CAST(CreateDate AS date) = CAST(GETDATE() AS date)
                                    OR CAST(UpdateDate AS date) = CAST(GETDATE() AS date)
                                    ) head
                                    LEFT JOIN RDR1 LINE ON head.DocEntry = line.DocEntry
                                    WHERE line.ItemCode IS NOT NULL
                                    
                                    UNION
                                    
                                    SELECT
                                        line.ItemCode AS id,
                                        CONCAT('AR-', FORMAT(head.UpdateDate, 'yyyyMMdd'), head.UpdateTS) AS serial
                                    FROM (
                                    SELECT DocEntry, UpdateDate, UpdateTS
                                    FROM OINV WITH(NOLOCK)
                                    WHERE CAST(CreateDate AS date) = CAST(GETDATE() AS date)
                                    OR CAST(UpdateDate AS date) = CAST(GETDATE() AS date)
                                    ) head
                                    LEFT JOIN INV1 LINE ON head.DocEntry = line.DocEntry
                                    WHERE line.ItemCode IS NOT NULL

                                    UNION

                                    SELECT 
                                        ItemCode AS id,
                                        CONCAT('BN-', FORMAT(CreateDate, 'yyyyMMdd'), CreateTS) AS serial
                                    FROM OITM 
                                    WHERE ItemCode IS NOT NULL
                                    AND CAST(CreateDate AS date) = CAST(GETDATE() AS date)
                                    AND (
                                        EXISTS(SELECT 1 FROM [@PCTP_POD] WITH(NOLOCK) WHERE U_BookingNumber = ItemCode)
                                        OR EXISTS(SELECT 1 FROM [@PCTP_PRICING] WITH(NOLOCK) WHERE U_BookingId = ItemCode)
                                    )
                                    AND (
                                        NOT EXISTS(SELECT 1 FROM SUMMARY_EXTRACT WITH(NOLOCK) WHERE U_BookingNumber = ItemCode)
                                        OR NOT EXISTS(SELECT 1 FROM POD_EXTRACT WITH(NOLOCK) WHERE U_BookingNumber = ItemCode)
                                        OR NOT EXISTS(SELECT 1 FROM BILLING_EXTRACT WITH(NOLOCK) WHERE U_BookingId = ItemCode)
                                        OR NOT EXISTS(SELECT 1 FROM TP_EXTRACT WITH(NOLOCK) WHERE U_BookingId = ItemCode)
                                        OR NOT EXISTS(SELECT 1 FROM PRICING_EXTRACT WITH(NOLOCK) WHERE U_BookingId = ItemCode)
                                    )
                                `, async (err: Error | undefined, recordset: sql.IResult<unknown> | undefined) => {
                                    if (err) {
                                        console.log(err)
                                        return;
                                    }
                                    if (!!recordset) {
                                        recordset.recordset.forEach(eventId => fetchedIds.push(eventId as EventId));
                                    }
                                    resolve();
                                });
                            })
                        })(pool);
                        await sapEventListener.processFetchedIds(pool, fetchedIds);
                        resolve()
                    }).catch(function (err) {
                        console.error('Error creating connection pool', err)
                    });
                })
            })(SAPEventListener.livePool, this);
            await TimeUtil.timeout(this.timeOut);
        }
    }

    private async processFetchedIds(livePool: sql.ConnectionPool, fetchedIds: EventId[]) {
        const fetchedIdsToProcess: EventId[] = [];
        for (const eventId of fetchedIds as EventId[]) {
            const { id, serial } = eventId;
            if (SAPEventListener.processedIds.includes(`${id}-${serial}`)) continue;
            fetchedIdsToProcess.push(eventId);
        }
        for (const { id, serial } of fetchedIdsToProcess) {
            // const paramObj: {name: string, type: sql.ISqlTypeWithLength, value: any}[] = [{ name: 'id', type: sql.VarChar(50), value: id }];
            await this.executeQuery(livePool, `
                UPDATE [@FirstratesTP] 
                SET U_Amount = NULL
                WHERE U_Amount = 'NaN' AND U_BN IN ('${id}');
            `);
            await this.executeQuery(livePool, `
                UPDATE [@FirstratesTP] 
                SET U_AddlAmount = NULL
                WHERE U_AddlAmount = 'NaN' AND U_BN IN ('${id}');
            `);
            await this.executeQuery(livePool, `
                -----> SUMMARY
                DELETE FROM SUMMARY_EXTRACT WHERE U_BookingNumber IN ('${id}');
            `);
            await this.executeQuery(livePool, `
                INSERT INTO SUMMARY_EXTRACT
                SELECT
                    X.Code, X.U_BookingNumber, X.U_BookingDate, X.U_ClientName, X.U_SAPClient, X.U_ClientVatStatus, X.U_TruckerName, X.U_SAPTrucker, X.U_TruckerVatStatus, X.U_VehicleTypeCap, X.U_ISLAND, X.U_ISLAND_D, X.U_IFINTERISLAND, X.U_DeliveryStatus, X.U_DeliveryDateDTR,
                    X.U_DeliveryDatePOD, X.U_ClientReceivedDate, X.U_ActualDateRec_Intitial, X.U_InitialHCRecDate, X.U_ActualHCRecDate, X.U_DateReturned, X.U_VerifiedDateHC, X.U_PTFNo, X.U_DateForwardedBT, X.U_PODSONum, X.U_GrossClientRates,
                    X.U_GrossClientRatesTax, X.U_GrossTruckerRates, X.U_GrossTruckerRatesTax, X.U_GrossProfitNet, X.U_TotalInitialClient, X.U_TotalInitialTruckers, X.U_TotalGrossProfit, X.U_BillingStatus, X.U_PODStatusPayment, X.U_PaymentReference,
                    X.U_PaymentStatus, X.U_ProofOfPayment, X.U_TotalRecClients, X.U_TotalPayable, X.U_PVNo, X.U_TotalAR, X.U_VarAR, X.U_TotalAP, X.U_VarTP, X.U_APDocNum, X.U_ARDocNum, X.U_DeliveryOrigin, X.U_Destination, X.U_PODStatusDetail, X.U_Remarks, X.U_WaybillNo, X.U_ServiceType,
                    X.U_InvoiceNo
                FROM [dbo].fetchPctpDataRows('SUMMARY', '${id}', DEFAULT) X;
            `);
            await this.executeQuery(livePool, `
                -----> POD
                DELETE FROM POD_EXTRACT WHERE U_BookingNumber IN ('${id}');
            `);
            await this.executeQuery(livePool, `
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
                FROM [dbo].fetchPctpDataRows('POD', '${id}', DEFAULT) X;
            `);
            await this.executeQuery(livePool, `
                -----> BILLING
                DELETE FROM BILLING_EXTRACT WHERE U_BookingNumber IN ('${id}');
            `);
            await this.executeQuery(livePool, `
                INSERT INTO BILLING_EXTRACT
                SELECT
                    X.U_BookingNumber, X.DisableTableRow, X.DisableSomeFields, X.Code, X.U_BookingId, X.U_BookingDate, X.U_PODNum, X.U_PODSONum, X.U_CustomerName, X.U_SAPClient, X.U_PlateNumber, X.U_VehicleTypeCap, X.U_DeliveryStatus, X.U_DeliveryDatePOD,
                    X.U_NoOfDrops, X.U_TripType, X.U_ClientReceivedDate, X.U_ActualHCRecDate, X.U_PODinCharge, X.U_VerifiedDateHC, X.U_PTFNo, X.U_DateForwardedBT, X.U_BillingDeadline, X.U_BillingStatus, X.U_BillingTeam, X.U_GrossInitialRate, X.U_Demurrage,
                    X.U_AddCharges, X.U_ActualBilledRate, X.U_RateAdjustments, X.U_ActualDemurrage, X.U_ActualAddCharges, X.U_TotalRecClients, X.U_CheckingTotalBilled, X.U_Checking, X.U_CWT2307, X.U_SOBNumber, X.U_ForwardLoad, X.U_BackLoad,
                    X.U_TypeOfAccessorial, X.U_TimeInEmptyDem, X.U_TimeOutEmptyDem, X.U_VerifiedEmptyDem, X.U_TimeInLoadedDem, X.U_TimeOutLoadedDem, X.U_VerifiedLoadedDem, X.U_TimeInAdvLoading, X.U_DayOfTheWeek, X.U_TimeIn, X.U_TimeOut,
                    X.U_TotalExceed, X.U_ODOIn, X.U_ODOOut, X.U_TotalUsage, X.U_SOLineNum, X.U_ARInvLineNum, X.U_TotalAR, X.U_VarAR, X.U_ServiceType, X.U_DocNum, X.U_InvoiceNo, X.U_DeliveryReceiptNo, X.U_SeriesNo, X.U_GroupProject, X.U_DeliveryOrigin,
                    X.U_Destination, X.U_OtherPODDoc, X.U_RemarksPOD, X.U_PODStatusDetail, X.U_BTRemarks, X.U_DestinationClient, X.U_Remarks, X.U_Attachment, X.U_SI_DRNo, X.U_TripTicketNo, X.U_WaybillNo, X.U_ShipmentManifestNo, X.U_OutletNo, X.U_CBM,
                    X.U_DeliveryMode, X.U_SourceWhse, X.U_SONo, X.U_NameCustomer, X.U_CategoryDR, X.U_IDNumber, X.U_Status, X.U_TotalInvAmount
                FROM [dbo].fetchPctpDataRows('BILLING', '${id}', DEFAULT) X;
            `);
            await this.executeQuery(livePool, `
                -----> TP
                DELETE FROM TP_EXTRACT WHERE U_BookingNumber IN ('${id}');
            `);
            await this.executeQuery(livePool, `
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
                FROM [dbo].fetchPctpDataRows('TP', '${id}', DEFAULT) X;
            `);
            await this.executeQuery(livePool, `
                -----> PRICING
                DELETE FROM PRICING_EXTRACT WHERE U_BookingNumber IN ('${id}');
            `);
            await this.executeQuery(livePool, `
                INSERT INTO PRICING_EXTRACT
                SELECT
                    X.U_BookingNumber, X.DisableSomeFields, X.DisableSomeFields2, X.Code, X.U_BookingId, X.U_BookingDate, X.U_PODNum, X.U_CustomerName, X.U_ClientTag, X.U_ClientProject, X.U_TruckerName, X.U_TruckerTag, X.U_VehicleTypeCap, X.U_DeliveryStatus,
                    X.U_TripType, X.U_NoOfDrops, X.U_GrossClientRates, X.U_ISLAND, X.U_ISLAND_D, X.U_IFINTERISLAND, X.U_GrossClientRatesTax, X.U_RateBasis, X.U_TaxType, X.U_GrossProfitNet, X.U_Demurrage, X.U_AddtlDrop, X.U_BoomTruck, X.U_Manpower, X.U_Backload,
                    X.U_TotalAddtlCharges, X.U_Demurrage2, X.U_AddtlDrop2, X.U_BoomTruck2, X.U_Manpower2, X.U_Backload2, X.U_totalAddtlCharges2, X.U_Demurrage3, X.U_AddtlCharges, X.U_GrossProfit, X.U_TotalInitialClient, X.U_TotalInitialTruckers, X.U_TotalGrossProfit,
                    X.U_ClientTag2, X.U_GrossTruckerRates, X.U_GrossTruckerRatesTax, X.U_RateBasisT, X.U_TaxTypeT, X.U_Demurrage4, X.U_AddtlCharges2, X.U_GrossProfitC, X.U_ActualBilledRate, X.U_BillingRateAdjustments,
                    X.U_BillingActualDemurrage, X.U_ActualAddCharges, X.U_TotalRecClients, X.U_TotalAR, X.U_VarAR, X.U_PODSONum, X.U_ActualRates, X.U_TPRateAdjustments, X.U_TPActualDemurrage, X.U_ActualCharges, X.U_TPBoomTruck2, X.U_OtherCharges,
                    X.U_TotalPayable, X.U_PVNo, X.U_TotalAP, X.U_VarTP, X.U_APDocNum, X.U_Paid, X.U_DocNum, X.U_DeliveryOrigin, X.U_Destination, X.U_RemarksDTR, X.U_RemarksPOD, X.U_PODDocNum
                FROM [dbo].fetchPctpDataRows('PRICING', '${id}', DEFAULT) X;
            `);
            SAPEventListener.processedIds.push(`${id}-${serial}`);
        }
        console.log(SAPEventListener.processedIds, SAPEventListener.processedIds.length, (new Date()).toString());
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
                            console.log(err)
                            resolve(false);
                        }
                        resolve(true);
                    });
                }
            })
        })(pool)
    }
}