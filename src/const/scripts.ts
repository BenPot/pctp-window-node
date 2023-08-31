export enum Script {
    POD = ` -- POD
            -- WaybillNo
            -- Waybill #

            -- ShipmentManifestNo
            -- Shipment No / Manifest No

            -- DeliveryReceiptNo
            -- Delivery Receipt No

            -- SeriesNo
            -- Series #

            SELECT
                --COLUMNS
                CASE
                    WHEN EXISTS(SELECT 1 FROM OINV H, INV1 L WHERE H.DocEntry = L.DocEntry AND L.ItemCode = T0.U_BookingNumber AND H.CANCELED = 'N')
                        AND EXISTS(
                            SELECT 1 
                            FROM OPCH H, PCH1 L 
                            WHERE H.DocEntry = L.DocEntry AND H.CANCELED = 'N' 
                            AND (L.ItemCode = T0.U_BookingNumber
                            OR (REPLACE(REPLACE(RTRIM(LTRIM(tp.U_PVNo)), ' ', ''), ',', '') LIKE '%' + RTRIM(LTRIM(H.U_PVNo)) + '%')))
                    THEN 'Y'
                    ELSE 'N'
                END AS DisableTableRow,
                -- T0.Code,
                T0.U_BookingNumber AS Code,
                T0.U_BookingDate,
                T0.U_BookingNumber,
                CASE
                    WHEN EXISTS(SELECT 1 FROM ORDR header WHERE header.CANCELED = 'N' AND header.DocEntry = billing.U_PODSONum) THEN billing.U_PODSONum
                    ELSE ''
                END AS U_PODSONum,
                -- billing.U_PODSONum AS U_PODSONum,
                -- T0.U_ClientName,
                client.CardName AS U_ClientName,
                T0.U_SAPClient,
                -- T0.U_TruckerName,
                trucker.CardName AS U_TruckerName,
                T0.U_ISLAND, 
                T0.U_ISLAND_D, 
                T0.U_IFINTERISLAND, 
                T0.U_VERIFICATION_TAT,
                T0.U_POD_TAT,
                T0.U_ActualDateRec_Intitial,
                T0.U_SAPTrucker,
                T0.U_PlateNumber,
                T0.U_VehicleTypeCap,
                T0.U_DeliveryStatus,
                T0.U_DeliveryDateDTR,
                T0.U_DeliveryDatePOD,
                T0.U_NoOfDrops,
                T0.U_TripType,
                -- T0.U_DocNum,
                -- T0.U_TripTicketNo,
                -- T0.U_WaybillNo,
                -- T0.U_ShipmentNo,
                -- T0.U_DeliveryReceiptNo,
                -- T0.U_SeriesNo,
                T0.U_Receivedby,
                T0.U_ClientReceivedDate,
                T0.U_InitialHCRecDate,
                T0.U_ActualHCRecDate,
                T0.U_DateReturned,
                T0.U_PODinCharge,
                T0.U_VerifiedDateHC,
                T0.U_PTFNo,
                T0.U_DateForwardedBT,
                T0.U_BillingDeadline,
                T0.U_BillingStatus,
                T0.U_ServiceType,
                T0.U_SINo,
                T0.U_BillingTeam,
                T0.U_SOBNumber,
                -- T0.U_OutletNo,
                -- T0.U_CBM,
                -- T0.U_SI_DRNo,
                -- T0.U_DeliveryMode,
                -- T0.U_SourceWhse,
                -- T0.U_TotalInvAmount,
                -- T0.U_SONo,
                -- T0.U_NameCustomer,
                -- T0.U_CategoryDR,
                T0.U_ForwardLoad,
                T0.U_BackLoad,
                -- T0.U_IDNumber,
                T0.U_TypeOfAccessorial,
                -- T0.U_ApprovalStatus,
                T0.U_TimeInEmptyDem,
                T0.U_TimeOutEmptyDem,
                T0.U_VerifiedEmptyDem,
                T0.U_TimeInLoadedDem,
                T0.U_TimeOutLoadedDem,
                T0.U_VerifiedLoadedDem,
                T0.U_TimeInAdvLoading,
                T0.U_PenaltiesManual,
                CASE WHEN ISNULL(T0.U_DayOfTheWeek,'') = '' THEN DATENAME(dw, T0.U_BookingDate)
                ELSE T0.U_DayOfTheWeek END AS 'U_DayOfTheWeek',
                T0.U_TimeIn,
                T0.U_TimeOut,
                T0.U_TotalNoExceed,
                T0.U_ODOIn,
                T0.U_ODOOut,
                T0.U_TotalUsage,
                CASE WHEN ISNULL(T0.U_ClientReceivedDate,'') = '' THEN 'PENDING' 
                ELSE 'SUBMITTED' 
                END AS 'U_ClientSubStatus', 
                -- CASE WHEN ISNULL(T0.U_ClientReceivedDate,'') != '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN DATEDIFF(day, T0.U_ClientReceivedDate, T0.U_DeliveryDateDTR) + ISNULL(client.U_DCD,0) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_WaivedDays), 0),'0') AS int) 
                -- ELSE 0 END AS 'U_ClientSubOverdue', 
                dbo.computeClientSubOverdue(
                    T0.U_DeliveryDateDTR,
                    T0.U_ClientReceivedDate,
                    ISNULL(T0.U_WaivedDays, 0),
                    CAST(ISNULL(client.U_DCD,0) as int)
                ) AS 'U_ClientSubOverdue', 
                -- CASE WHEN ISNULL(T0.U_ClientReceivedDate,'') != '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN 
                --     (CASE WHEN (DATEDIFF(day, T0.U_ClientReceivedDate, T0.U_DeliveryDateDTR) + ISNULL(client.U_DCD,0)) < 0 THEN (DATEDIFF(day, T0.U_ClientReceivedDate, T0.U_DeliveryDateDTR) + ISNULL(client.U_DCD,0)) * 200 ELSE 0 END)
                -- ELSE 0 END AS 'U_ClientPenaltyCalc',
                dbo.computeClientPenaltyCalc(
                    dbo.computeClientSubOverdue(
                        T0.U_DeliveryDateDTR,
                        T0.U_ClientReceivedDate,
                        ISNULL(T0.U_WaivedDays, 0),
                        CAST(ISNULL(client.U_DCD,0) as int)
                    )
                ) AS 'U_ClientPenaltyCalc',
                -- CASE 
                -- WHEN ISNULL(T0.U_ActualHCRecDate,'') = '' AND ISNULL(T0.U_DeliveryDateDTR,'') != ''  THEN 
                --     (CASE WHEN DATEDIFF(day, GETDATE(), DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_WaivedDays), 0),'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_HolidayOrWeekend), 0),'0') AS int), T0.U_DeliveryDateDTR)) >= 0 THEN 'Ontime' 
                --         WHEN DATEDIFF(day, GETDATE(), DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_WaivedDays), 0),'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_HolidayOrWeekend), 0),'0') AS int), T0.U_DeliveryDateDTR)) BETWEEN -13 AND 0 THEN 'Late'
                --         WHEN DATEDIFF(day, GETDATE(), DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_WaivedDays), 0),'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_HolidayOrWeekend), 0),'0') AS int), T0.U_DeliveryDateDTR)) <= -13 THEN 'Lost'
                --     END)
                -- WHEN ISNULL(T0.U_ActualHCRecDate,'') != '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN 
                --     (CASE WHEN DATEDIFF(day, T0.U_ActualHCRecDate, DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_WaivedDays), 0),'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_HolidayOrWeekend), 0),'0') AS int), T0.U_DeliveryDateDTR)) >= 0 THEN 'Ontime'
                --         WHEN DATEDIFF(day, T0.U_ActualHCRecDate, DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_WaivedDays), 0),'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_HolidayOrWeekend), 0),'0') AS int), T0.U_DeliveryDateDTR)) BETWEEN -13 AND 0 THEN 'Late'
                --         WHEN DATEDIFF(day, T0.U_ActualHCRecDate, DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_WaivedDays), 0),'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_HolidayOrWeekend), 0),'0') AS int), T0.U_DeliveryDateDTR)) <= -13 THEN 'Lost'
                --     END)
                -- ELSE '' END AS 'U_PODStatusPayment',
                dbo.computePODStatusPayment(
                    dbo.computeOverdueDays(
                        T0.U_ActualHCRecDate,
                        dbo.computePODSubmitDeadline(
                            T0.U_DeliveryDateDTR,
                            ISNULL(client.U_CDC,0)
                        ),
                        ISNULL(T0.U_HolidayOrWeekend, 0)
                    )
                ) AS 'U_PODStatusPayment',
                --T0.U_PODStatusPayment,
                -- CASE WHEN ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN DATEADD(day, CAST(ISNULL(client.U_CDC,0) AS int) , T0.U_DeliveryDateDTR)
                -- ELSE T0.U_PODSubmitDeadline END AS 'U_PODSubmitDeadline',
                dbo.computePODSubmitDeadline(
                    T0.U_DeliveryDateDTR,
                    ISNULL(client.U_CDC,0)
                ) AS 'U_PODSubmitDeadline',
                -- CASE 
                --     WHEN ISNULL(T0.U_ActualHCRecDate,'') = '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN DATEDIFF(day, GETDATE(), DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_HolidayOrWeekend), 0),'0') AS int), T0.U_DeliveryDateDTR))
                --     WHEN ISNULL(T0.U_ActualHCRecDate,'') != '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN DATEDIFF(day, T0.U_ActualHCRecDate, DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) + CAST(ISNULL(NULLIF(ISNUMERIC(T0.U_HolidayOrWeekend), 0),'0') AS int), T0.U_DeliveryDateDTR)) 
                -- ELSE 0 END AS 'U_OverdueDays',
                dbo.computeOverdueDays(
                    T0.U_ActualHCRecDate,
                    dbo.computePODSubmitDeadline(
                        T0.U_DeliveryDateDTR,
                        ISNULL(client.U_CDC,0)
                    ),
                    ISNULL(T0.U_HolidayOrWeekend, 0)
                ) AS 'U_OverdueDays',
                --T0.U_OverdueDays,
                -- CASE 
                -- WHEN ISNULL(T0.U_InitialHCRecDate,'') = '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN 
                --     (CASE WHEN DATEDIFF(day, GETDATE(), DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) , T0.U_DeliveryDateDTR)) BETWEEN -13 AND 0 THEN DATEDIFF(day, GETDATE(), DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) , T0.U_DeliveryDateDTR)) * 200 ELSE 0 END)
                -- WHEN ISNULL(T0.U_InitialHCRecDate,'') != '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN 
                --     (CASE WHEN DATEDIFF(day, T0.U_InitialHCRecDate, DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) , T0.U_DeliveryDateDTR)) BETWEEN -13 AND 0 THEN DATEDIFF(day, T0.U_InitialHCRecDate, DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) , T0.U_DeliveryDateDTR)) * 200 ELSE 0 END)
                -- ELSE 0 END AS 'U_InteluckPenaltyCalc',
                dbo.computeInteluckPenaltyCalc(
                    dbo.computePODStatusPayment(
                        dbo.computeOverdueDays(
                            T0.U_ActualHCRecDate,
                            dbo.computePODSubmitDeadline(
                                T0.U_DeliveryDateDTR,
                                ISNULL(client.U_CDC,0)
                            ),
                            ISNULL(T0.U_HolidayOrWeekend, 0)
                        )
                    ),
                    dbo.computeOverdueDays(
                        T0.U_ActualHCRecDate,
                        dbo.computePODSubmitDeadline(
                            T0.U_DeliveryDateDTR,
                            ISNULL(client.U_CDC,0)
                        ),
                        ISNULL(T0.U_HolidayOrWeekend, 0)
                    )
                ) AS 'U_InteluckPenaltyCalc',
                T0.U_WaivedDays,
                T0.U_HolidayOrWeekend,
                -- CASE 
                -- WHEN ISNULL(T0.U_InitialHCRecDate,'') = '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN 
                --     (CASE WHEN DATEDIFF(day, GETDATE(), DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) , T0.U_DeliveryDateDTR)) <= -13 THEN (CASE WHEN ISNULL(pricing.U_TotalInitialTruckers,0) <> 0 THEN -(pricing.U_TotalInitialTruckers * 2) ELSE 0 END) END )
                -- WHEN ISNULL(T0.U_InitialHCRecDate,'') != '' AND ISNULL(T0.U_DeliveryDateDTR,'') != '' THEN 
                --     (CASE WHEN DATEDIFF(day, T0.U_InitialHCRecDate, DATEADD(day, CAST(ISNULL(client.U_CDC,'0') AS int) , T0.U_DeliveryDateDTR)) <= -13 THEN (CASE WHEN ISNULL(pricing.U_TotalInitialTruckers,0) <> 0 THEN -(pricing.U_TotalInitialTruckers * 2) ELSE 0 END) END )
                -- ELSE 0 END AS 'U_LostPenaltyCalc',
                dbo.computeLostPenaltyCalc(
                    dbo.computePODStatusPayment(
                        dbo.computeOverdueDays(
                            T0.U_ActualHCRecDate,
                            dbo.computePODSubmitDeadline(
                                T0.U_DeliveryDateDTR,
                                ISNULL(client.U_CDC,0)
                            ),
                            ISNULL(T0.U_HolidayOrWeekend, 0)
                        )
                    ),
                    T0.U_InitialHCRecDate,
                    T0.U_DeliveryDateDTR,
                    CASE
                        WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN pricing.U_GrossTruckerRates
                        WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN (pricing.U_GrossTruckerRates / 1.12)
                    END + CASE
                        WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN pricing.U_Demurrage2
                        WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN (pricing.U_Demurrage2 / 1.12)
                    END + CASE
                        WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN (pricing.U_AddtlDrop + pricing.U_BoomTruck + pricing.U_Manpower + pricing.U_Backload)
                        WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN ((pricing.U_AddtlDrop + pricing.U_BoomTruck + pricing.U_Manpower + pricing.U_Backload) / 1.12)
                    END
                ) AS 'U_LostPenaltyCalc',
                -- T0.U_TotalSubPenalties,
                dbo.computeTotalSubPenalties(
                    dbo.computeClientPenaltyCalc(
                        dbo.computeClientSubOverdue(
                            T0.U_DeliveryDateDTR,
                            T0.U_ClientReceivedDate,
                            ISNULL(T0.U_WaivedDays, 0),
                            CAST(ISNULL(client.U_DCD,0) as int)
                        )
                    ),
                    dbo.computeInteluckPenaltyCalc(
                        dbo.computePODStatusPayment(
                            dbo.computeOverdueDays(
                                T0.U_ActualHCRecDate,
                                dbo.computePODSubmitDeadline(
                                    T0.U_DeliveryDateDTR,
                                    ISNULL(client.U_CDC,0)
                                ),
                                ISNULL(T0.U_HolidayOrWeekend, 0)
                            )
                        ),
                        dbo.computeOverdueDays(
                            T0.U_ActualHCRecDate,
                            dbo.computePODSubmitDeadline(
                                T0.U_DeliveryDateDTR,
                                ISNULL(client.U_CDC,0)
                            ),
                            ISNULL(T0.U_HolidayOrWeekend, 0)
                        )
                    ),
                    dbo.computeLostPenaltyCalc(
                        dbo.computePODStatusPayment(
                            dbo.computeOverdueDays(
                                T0.U_ActualHCRecDate,
                                dbo.computePODSubmitDeadline(
                                    T0.U_DeliveryDateDTR,
                                    ISNULL(client.U_CDC,0)
                                ),
                                ISNULL(T0.U_HolidayOrWeekend, 0)
                            )
                        ),
                        T0.U_InitialHCRecDate,
                        T0.U_DeliveryDateDTR,
                        CASE
                            WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN pricing.U_GrossTruckerRates
                            WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN (pricing.U_GrossTruckerRates / 1.12)
                        END + CASE
                            WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN pricing.U_Demurrage2
                            WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN (pricing.U_Demurrage2 / 1.12)
                        END + CASE
                            WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN (pricing.U_AddtlDrop + pricing.U_BoomTruck + pricing.U_Manpower + pricing.U_Backload)
                            WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN ((pricing.U_AddtlDrop + pricing.U_BoomTruck + pricing.U_Manpower + pricing.U_Backload) / 1.12)
                        END
                    ),
                    ISNULL(T0.U_PenaltiesManual,0)
                ) AS U_TotalSubPenalties,
                CASE WHEN ISNULL(T0.U_Waived,'') = '' THEN 'N' 
                ELSE T0.U_Waived END AS 'U_Waived',
                T0.U_PercPenaltyCharge,
                T0.U_Approvedby,
                -- T0.U_TotalPenaltyWaived,
                dbo.computeTotalPenaltyWaived(
                    dbo.computeTotalSubPenalties(
                        dbo.computeClientPenaltyCalc(
                            dbo.computeClientSubOverdue(
                                T0.U_DeliveryDateDTR,
                                T0.U_ClientReceivedDate,
                                ISNULL(T0.U_WaivedDays, 0),
                                CAST(ISNULL(client.U_DCD,0) as int)
                            )
                        ),
                        dbo.computeInteluckPenaltyCalc(
                            dbo.computePODStatusPayment(
                                dbo.computeOverdueDays(
                                    T0.U_ActualHCRecDate,
                                    dbo.computePODSubmitDeadline(
                                        T0.U_DeliveryDateDTR,
                                        ISNULL(client.U_CDC,0)
                                    ),
                                    ISNULL(T0.U_HolidayOrWeekend, 0)
                                )
                            ),
                            dbo.computeOverdueDays(
                                T0.U_ActualHCRecDate,
                                dbo.computePODSubmitDeadline(
                                    T0.U_DeliveryDateDTR,
                                    ISNULL(client.U_CDC,0)
                                ),
                                ISNULL(T0.U_HolidayOrWeekend, 0)
                            )
                        ),
                        dbo.computeLostPenaltyCalc(
                            dbo.computePODStatusPayment(
                                dbo.computeOverdueDays(
                                    T0.U_ActualHCRecDate,
                                    dbo.computePODSubmitDeadline(
                                        T0.U_DeliveryDateDTR,
                                        ISNULL(client.U_CDC,0)
                                    ),
                                    ISNULL(T0.U_HolidayOrWeekend, 0)
                                )
                            ),
                            T0.U_InitialHCRecDate,
                            T0.U_DeliveryDateDTR,
                            CASE
                                WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN pricing.U_GrossTruckerRates
                                WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN (pricing.U_GrossTruckerRates / 1.12)
                            END + CASE
                                WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN pricing.U_Demurrage2
                                WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN (pricing.U_Demurrage2 / 1.12)
                            END + CASE
                                WHEN ISNULL(trucker.VatStatus,'Y') = 'Y' THEN (pricing.U_AddtlDrop + pricing.U_BoomTruck + pricing.U_Manpower + pricing.U_Backload)
                                WHEN ISNULL(trucker.VatStatus,'Y') = 'N' THEN ((pricing.U_AddtlDrop + pricing.U_BoomTruck + pricing.U_Manpower + pricing.U_Backload) / 1.12)
                            END
                        ),
                        ISNULL(T0.U_PenaltiesManual,0)
                    ),
                    ISNULL(T0.U_PercPenaltyCharge,0)
                ) AS U_TotalPenaltyWaived,
                ISNULL(billing.Code,'') 'BillingNum',
                ISNULL(tp.Code,'') 'TPNum',
                ISNULL(pricing.Code,'') 'PricingNum',
                ISNULL(client.U_CDC,0) 'CDC',
                ISNULL(client.U_DCD,0) 'DCD',
                ISNULL(pricing.U_GrossTruckerRates,0) 'GrossTruckerRates',
                ISNULL(CAST(client.U_GroupLocation as nvarchar(max)), '') AS U_GroupProject,
                CAST(T0.U_Attachment as nvarchar(max)) AS U_Attachment,
                CAST(T0.U_DeliveryOrigin as nvarchar(max)) AS U_DeliveryOrigin,
                CAST(T0.U_Destination as nvarchar(max)) AS U_Destination,
                CAST(T0.U_Remarks as nvarchar(max)) AS U_Remarks,
                CAST(T0.U_OtherPODDoc as nvarchar(max)) AS U_OtherPODDoc,
                CAST(T0.U_RemarksPOD as nvarchar(max)) AS U_RemarksPOD,
                CAST(T0.U_PODStatusDetail as nvarchar(max)) AS U_PODStatusDetail,
                CAST(T0.U_BTRemarks as nvarchar(max)) AS U_BTRemarks,
                CAST(T0.U_DestinationClient as nvarchar(max)) AS U_DestinationClient,
                CAST(T0.U_Remarks2 as nvarchar(max)) AS U_Remarks2,
                CAST(T0.U_DocNum as nvarchar(max)) AS U_DocNum,
                CAST(T0.U_TripTicketNo as nvarchar(max)) AS U_TripTicketNo,
                CAST(T0.U_WaybillNo as nvarchar(max)) AS U_WaybillNo,
                CAST(T0.U_ShipmentNo as nvarchar(max)) AS U_ShipmentNo,
                CAST(T0.U_DeliveryReceiptNo as nvarchar(max)) AS U_DeliveryReceiptNo,
                CAST(T0.U_SeriesNo as nvarchar(max)) AS U_SeriesNo,
                CAST(T0.U_OutletNo as nvarchar(max)) AS U_OutletNo,
                CAST(T0.U_CBM as nvarchar(max)) AS U_CBM,
                CAST(T0.U_SI_DRNo as nvarchar(max)) AS U_SI_DRNo,
                CAST(T0.U_DeliveryMode as nvarchar(max)) AS U_DeliveryMode,
                CAST(T0.U_SourceWhse as nvarchar(max)) AS U_SourceWhse,
                CAST(T0.U_SONo as nvarchar(max)) AS U_SONo,
                CAST(T0.U_NameCustomer as nvarchar(max)) AS U_NameCustomer,
                CAST(T0.U_CategoryDR as nvarchar(max)) AS U_CategoryDR,
                CAST(T0.U_IDNumber as nvarchar(max)) AS U_IDNumber,
                CAST(T0.U_ApprovalStatus as nvarchar(max)) AS U_ApprovalStatus,
                CAST(T0.U_TotalInvAmount as nvarchar(max)) AS U_TotalInvAmount

                --COLUMNS
            FROM [dbo].[@PCTP_POD] T0 
            LEFT JOIN [dbo].[@PCTP_BILLING] billing ON T0.U_BookingNumber = billing.U_BookingId
            LEFT JOIN [dbo].[@PCTP_TP] tp ON T0.U_BookingNumber = tp.U_BookingId
            LEFT JOIN [dbo].[@PCTP_PRICING] pricing ON T0.U_BookingNumber = pricing.U_BookingId
            LEFT JOIN OCRD client ON T0.U_SAPClient = client.CardCode
            LEFT JOIN OCRD trucker ON T0.U_SAPTrucker = trucker.CardCode
        `,
}