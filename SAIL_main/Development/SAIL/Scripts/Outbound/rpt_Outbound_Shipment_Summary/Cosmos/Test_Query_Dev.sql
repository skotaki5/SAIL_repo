--AUTHOR : 		VISHAL SHARMA
--DESCRIPTION:	rpt_Outbound_Shipment_Summary
--DATE : 		31-03-2022

Result Set 1
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 
--* Target Container-digital_summary_orders

SELECT count(1) Total
FROM (
    SELECT DISTINCT T.UPSOrderNumber
        ,T.upsTransportShipmentNumber  FROM T
    WHERE
        -- (T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000')))
         (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
    )

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'  
--* Target Container-digital_summary_orders

SELECT count(1) Total
FROM (
    SELECT DISTINCT T.UPSOrderNumber
        ,T.upsTransportShipmentNumber  FROM T
    WHERE
        -- (T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000')))
         (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main  BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
         
    )

Result Set 2
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 
--* Target Container-digital_summary_orders

SELECT a.milestoneStatus, count(1) milestoneStatusCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.milestoneStatus
from T
where T.milestoneStatus != null
 
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		) a
        group by a.milestoneStatus


 CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'  
--* Target Container-digital_summary_orders

SELECT a.milestoneStatus, count(1) milestoneStatusCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.milestoneStatus
from T
where T.milestoneStatus != null
 
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		) a
        group by a.milestoneStatus	

Result Set 3
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

SELECT  a.ShipmentMode,count(1) ShipmentModeCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.ShipmentMode
from T
where T.ShipmentMode != null
       -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
       --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
       AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
           OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
       AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
           OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
       AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
       -- AND (
       --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
       --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
       --  )
       AND (
           T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
               AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
           )
       -- AND (
       --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
       --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
       --  )
       AND T.IS_INBOUND = 0
       AND T.is_deleted = 0
       AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
       AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
	   --AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
	   )  a
       group by a.ShipmentMode

 CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*' 
--* Target Container-digital_summary_orders

SELECT  a.ShipmentMode,count(1) ShipmentModeCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.ShipmentMode
from T
where T.ShipmentMode != null
       -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
       --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
       AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
           OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
       AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
           OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
       AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
       -- AND (
       --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
       --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
       --  )
       AND (
           T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
               AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
           )
       -- AND (
       --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
       --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
       --  )
       AND T.IS_INBOUND = 0
       AND T.is_deleted = 0
       AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
       AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
	   --AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
	   )a
       group by a.ShipmentMode

Result Set 4
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 
--* Target Container-digital_summary_orders

SELECT   a.OriginCountry,count(1) OriginCountryCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.OriginCountry
from T
where T.OriginCountry != null
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		)  a
        group by a.OriginCountry

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*' 
--* Target Container-digital_summary_orders

SELECT   a.OriginCountry,count(1) OriginCountryCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.OriginCountry
from T
where T.OriginCountry != null
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		)a
        group by a.OriginCountry

Result Set 5
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 		
--* Target Container-digital_summary_orders

SELECT   a.DestinationCountry,count(1) DestinationCountryCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.DestinationCountry
from T
where T.DestinationCountry != null
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		)  a
        group by a.DestinationCountry

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*' 
--* Target Container-digital_summary_orders

SELECT   a.DestinationCountry,count(1) DestinationCountryCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.DestinationCountry
from T
where T.DestinationCountry != null
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		)a
        group by a.DestinationCountry	

Result Set 6
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*' 		
 --* Target Container-digital_summary_orders

SELECT   a.ServiceLevel,count(1) serviceLevelCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.ServiceLevel from T
where T.ServiceLevel != null
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		)a
        group by a.ServiceLevel

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*' 		
--* Target Container-digital_summary_orders

SELECT   a.ServiceLevel,count(1) serviceLevelCount from
(select Distinct T.UPSOrderNumber, T.upsTransportShipmentNumber ,T.ServiceLevel from T
where T.ServiceLevel != null
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		)a
        group by a.ServiceLevel

Result Set 7
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'		
 --* Target Container-digital_summary_orders

SELECT  count(1) TemperatureShipmentCount, a.ShipmentMode from 
(select Distinct T.UPSOrderNumber ,T.ShipmentMode from T
WHERE T.IS_TEMPERATURE='Y'     
AND T.STATUSDETAILTYPE='TemperatureTracking'
-- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
--          AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		) a
        group by a.ShipmentMode	

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'		
--* Target Container-digital_summary_orders

SELECT  count(1) TemperatureShipmentCount, a.ShipmentMode from 
(select Distinct T.UPSOrderNumber ,T.ShipmentMode from T
WHERE T.IS_TEMPERATURE='Y'     
AND T.STATUSDETAILTYPE='TemperatureTracking'
-- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
--          AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
		)a
        group by a.ShipmentMode	

Result Set 8
 CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
 --* Target Container-digital_summary_orders

-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater
--select top 5 * from ( to be done in BACKEND
         SELECT  T.Carrier
        ,T.deliveryStatus DeliveryStatus
        ,COUNT(1) AS Count
    FROM T
    WHERE
        T.milestoneStatus = 'DELIVERED'
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
        GROUP BY T.Carrier
        ,T.deliveryStatus 
--Order by Count DESC to be done in Backend	

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'	
--* Target Container-digital_summary_orders

-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater
--select top 5 * from ( to be done in BACKEND
SELECT  T.Carrier
        ,T.deliveryStatus DeliveryStatus
        ,COUNT(1) AS Count
    FROM T
    WHERE
        T.milestoneStatus = 'DELIVERED'
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
        GROUP BY T.Carrier
        ,T.deliveryStatus      
--Order by Count DESC to be done in Backend  

Result Set 9
-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater
 
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater  
--select top 5 * from ( to be done in BACKEND
SELECT UPPER(T.OriginCity) SourceCity
        ,UPPER(T.DestinationCity) DestinationCity
        ,UPPER(T.OriginCountry) SourceCountry
        ,UPPER(T.DestinationCountry) DestinationCountry
        ,UPPER(T.deliveryStatus) DeliveryStatus
        ,COUNT(1) Count
    FROM T
    WHERE
        (UPPER(T.OriginCity) NOT LIKE '%NOT AVAILABLE%'
        AND UPPER(T.DestinationCity) NOT LIKE '%NOT AVAILABLE%')
        AND T.milestoneStatus = 'DELIVERED'
         
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
        --AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes 
    GROUP BY UPPER(T.OriginCity)
        ,UPPER(T.DestinationCity)
        ,UPPER(T.OriginCountry)
        ,UPPER(T.DestinationCountry)
        ,UPPER(T.deliveryStatus)
--Order by Count DESC to be done in Backend

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'
--* Target Container-digital_summary_orders

-- MS Team :Need to have a look: Issue:TOP 5 AND ORDER BY need to cater    
--select top 5 * from ( to be done in BACKEND
SELECT UPPER(T.OriginCity) SourceCity
        ,UPPER(T.DestinationCity) DestinationCity
        ,UPPER(T.OriginCountry) SourceCountry
        ,UPPER(T.DestinationCountry) DestinationCountry
        ,UPPER(T.deliveryStatus) DeliveryStatus
        ,COUNT(1) Count
    FROM T
    WHERE
        (UPPER(T.OriginCity) NOT LIKE '%NOT AVAILABLE%'
        AND UPPER(T.DestinationCity) NOT LIKE '%NOT AVAILABLE%')
        AND T.milestoneStatus = 'DELIVERED'
         
        -- AND T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        --  AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000'))
        AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
        --AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes 
         
    GROUP BY UPPER(T.OriginCity)
        ,UPPER(T.DestinationCity)
        ,UPPER(T.OriginCountry)
        ,UPPER(T.DestinationCountry)
        ,UPPER(T.deliveryStatus)        
--Order by Count DESC to be done in Backend	

Result Set 10	
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

SELECT   
a.ShipmentMode,   
a.deliveryStatus DeliveryStatus,   
COUNT(1) AS Count FROM(
SELECT distinct T.ShipmentMode,T.deliveryStatus,T.UPSOrderNumber,T.upsTransportShipmentNumber 
FROM  T   
WHERE 
-- (T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
--      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000')))
         (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
        AND T.milestoneStatus = 'DELIVERED'
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
)a
GROUP BY    
a.ShipmentMode,   
a.deliveryStatus

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'
--* Target Container-digital_summary_orders

SELECT   
a.ShipmentMode,   
a.deliveryStatus DeliveryStatus,   
COUNT(1) AS Count FROM(
SELECT distinct T.ShipmentMode,T.deliveryStatus,T.UPSOrderNumber,T.upsTransportShipmentNumber   FROM  T   
WHERE 
--      (T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
--      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000')))
         (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
        AND T.milestoneStatus = 'DELIVERED'
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
)a
GROUP BY    
a.ShipmentMode,   
a.deliveryStatus
 
--ORDER BY Count DESC to be Applied at Backend   

Result Set 11	
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

  SELECT
    T.Carrier,
    COUNT(1) CarrierCount
	  --Sprint 52 Changes T.TemperatureThreshold removed
 FROM T
WHERE 
-- (T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
--      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000')))
         (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		AND T.Carrier != null
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
  GROUP BY T.Carrier
		    --Sprint 52 Changes T.TemperatureThreshold removed

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'
--* Target Container-digital_summary_orders

  SELECT
    T.Carrier,
    COUNT(1) CarrierCount
	  --Sprint 52 Changes T.TemperatureThreshold removed
 FROM T
WHERE 
        -- (T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        -- AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000')))
         (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
        AND T.Carrier != null
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
  GROUP BY T.Carrier
		    --Sprint 52 Changes T.TemperatureThreshold removed

Result Set 12	
CASE 1: IF @shipmentCreationStartDateTime IS NOT NULL
--* Target Container-digital_summary_orders

SELECT COUNT(1) as ScheduleToShipCount FROM T 
              WHERE T.milestoneStatus IN ('TRANSPORTATION PLANNING')
            --   AND (
            --      T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
            --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            --      )
              AND (
                    T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
                    )
              AND T.IS_INBOUND = 0
              AND T.is_deleted = 0
              AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
              OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
         
              AND (is_null(T.OrderStatusName) ? '' :T.OrderStatusName) <> 'Cancelled' 
              AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y' 

CASE 2: ELSE	
--* Target Container-digital_summary_orders

SELECT COUNT(1) as ScheduleToShipCount FROM T 
              WHERE T.milestoneStatus IN ('TRANSPORTATION PLANNING')
            --   AND (
            --      T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
            --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            --      )
              AND (
                    T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
                    )
              AND T.IS_INBOUND = 0
              AND T.is_deleted = 0
              AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
              OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
         
              AND (is_null(T.OrderStatusName) ? '' :T.OrderStatusName) <> 'Cancelled' 
              AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y' 			  


Result Set 13
CASE 1: IF @shipmentCreationStartDateTime IS NOT NULL
--* Target Container-digital_summary_orders

SELECT COUNT(1) as MissedPickupCount FROM T 
              WHERE T.milestoneStatus IN ('TRANSPORTATION PLANNING')
            --   AND (
            --      T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
            --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            --      )
              AND (
                    T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
                    )
              AND T.IS_INBOUND = 0
              AND T.is_deleted = 0
              AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
              OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
              AND (IS_null(T.OrderStatusName) ? '' :T.OrderStatusName) <> 'Cancelled' 
              AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'

CASE 2: ELSE		
--* Target Container-digital_summary_orders
SELECT COUNT(1) as MissedPickupCount FROM T 
              WHERE T.milestoneStatus IN ('TRANSPORTATION PLANNING')
            --   AND (
            --      T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
            --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            --      )
              AND (
                    T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                    AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
                    )
              AND T.IS_INBOUND = 0
              AND T.is_deleted = 0
              AND (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
              OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
              AND (IS_null(T.OrderStatusName) ? '' :T.OrderStatusName) <> 'Cancelled' 
              AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'			  
				
Result Set 14
CASE 1: IF @Date.shipmentCreationStartDateTime IS NOT NULL
--* Target Container-digital_summary_orders

SELECT SUM(T.MissedDeliveredCount) AS ScheduledToDeliverCount FROM T
             WHERE  
           --   AND (
           --      T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
           --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
           --      )
              (
                   T.DateTimeReceived BETWEEN '2022-01-20 00:00:00.000'
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-20 00:00:00.000'))
                   )
             AND  T.IS_INBOUND = 0
             AND T.is_deleted = 0
             AND (T.AccountId IN ('3ABD6764-6795-47C3-9DDB-F25E0DBCB6E0')
             OR T.AccountId = '3ABD6764-6795-47C3-9DDB-F25E0DBCB6E0') 
             AND T.MissedDeliveredCount>0                  				  

CASE 2 ELSE 
--* Target Container-digital_summary_orders

SELECT SUM(T.MissedDeliveredCount) AS  ScheduledToDeliverCount  FROM T
             WHERE 
           --   (
           --      T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
           --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
           --      )
             (
                   T.actualShipmentDateTime_main BETWEEN '2022-01-20 00:00:00.000'
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-20 00:00:00.000'))
                   )
             AND  T.IS_INBOUND = 0
           AND T.is_deleted = 0
            AND (T.AccountId IN ('3ABD6764-6795-47C3-9DDB-F25E0DBCB6E0')
             OR T.AccountId = '3ABD6764-6795-47C3-9DDB-F25E0DBCB6E0')
             AND T.MissedDeliveredCount>0

Result Set 15
CASE 1: IF @shipmentCreationStartDateTime IS NOT NULL
--* Target Container-digital_summary_orders
			  
   SELECT SUM(T.MissedDeliveredCount) AS MissedDeliveredCount FROM T
             WHERE  
           --   AND (
           --      T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
           --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
           --      )
              (
                   T.DateTimeReceived BETWEEN '2022-01-20 00:00:00.000'
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-20 00:00:00.000'))
                   )
             AND  T.IS_INBOUND = 0
             AND T.is_deleted = 0
             AND (T.AccountId IN ('3ABD6764-6795-47C3-9DDB-F25E0DBCB6E0')
             OR T.AccountId = '3ABD6764-6795-47C3-9DDB-F25E0DBCB6E0') 
             AND T.MissedDeliveredCount>0  

 CASE 2: ELSE		
--* Target Container-digital_summary_orders

SELECT SUM(T.MissedDeliveredCount) AS MissedDeliveredCount FROM T
             WHERE 
           --   (
           --      T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
           --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
           --      )
             (
                   T.actualShipmentDateTime_main BETWEEN '2022-01-20 00:00:00.000'
                   AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-03-20 00:00:00.000'))
                   )
             AND  T.IS_INBOUND = 0
             AND T.is_deleted = 0
             AND (T.AccountId IN ('3ABD6764-6795-47C3-9DDB-F25E0DBCB6E0')
             OR T.AccountId = '3ABD6764-6795-47C3-9DDB-F25E0DBCB6E0')
             AND T.MissedDeliveredCount>0   
			 
Result Set 16  --Sprint 52 Changes	
CASE 1: IF @DateType = 'SHIPMENTCREATIONDATE' and  @NULLshipmentshippedDate = '*'
--* Target Container-digital_summary_orders

  SELECT
    T.Carrier carrier,
    COUNT(1) count,
	T.TemperatureThreshold  temperatureThreshold  
 FROM T
WHERE 
-- (T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
--      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000')))
         (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.DateTimeReceived BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
		AND T.Carrier != null
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) 
  GROUP BY T.Carrier,
		    T.TemperatureThreshold 

CASE 2 :IF @DateType = 'SHIPMENTSHIPPEDDATE' and  @NULLCreatedDate = '*'
--* Target Container-digital_summary_orders

  SELECT
    T.Carrier carrier,
    COUNT(1) count,
	T.TemperatureThreshold  temperatureThreshold  
 FROM T
WHERE 
        -- (T.actualDeliveryDateTime BETWEEN '2021-11-01 00:00:00.000'
        -- AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-02 00:00:00.000')))
         (T.AccountId IN ('1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
            OR T.AccountId = '1EEF1B1A-A415-43F3-88C5-2D5EBC503529')
        AND (T.DP_SERVICELINE_KEY IN ('A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
            OR T.DP_SERVICELINE_KEY = 'A0A885C1-8A23-4218-A7A0-F7236ADBF4AD')
        AND T.FacilityId IN ('8BC005F6-08A4-40FB-A64A-08D6175D2695')
        -- AND (
        --  T.ScheduledPickUpDateTime BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND (
            T.actualShipmentDateTime_main BETWEEN '2022-02-01 00:00:00.000'
                AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
            )
        -- AND (
        --  T.LoadLatestDeliveryDate BETWEEN '2022-02-01 00:00:00.000'
        --      AND DateTimeAdd("ms", - 2, DateTimeAdd("dd", 1, '2022-02-15 00:00:00.000'))
        --  )
        AND T.IS_INBOUND = 0
        AND T.is_deleted = 0
        AND (is_null(T.OrderCancelledFlag) ? 'N' :T.OrderCancelledFlag) <> 'Y'
        AND (UPPER('Y') = 'Y' ? 1 : (UPPER('Y') = 'N' ? 0 : 'Y')) = T.is_managed
        AND T.Carrier != null
		--AND (UPPER(is_null(T.shipmentDescription) ? '' :T.shipmentDescription) IN ('B2B')) --Sprint 52 Changes
  GROUP BY T.Carrier,
		   T.TemperatureThreshold  		 
