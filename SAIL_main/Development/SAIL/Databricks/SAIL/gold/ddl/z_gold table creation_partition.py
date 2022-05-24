# Databricks notebook source
# MAGIC %sql
# MAGIC create external table fact_order_dim_inc(
# MAGIC UPS_ORDER_NUMBER string
# MAGIC ,CUSTOMER_ORDER_NUMBER string
# MAGIC ,SOURCE_SYSTEM_KEY integer
# MAGIC ,WAREHOUSE_KEY decimal(18,0)
# MAGIC ,IS_INBOUND integer
# MAGIC ,CUSTOMER_PO_NUMBER string
# MAGIC ,REFERENCE_ORDER_NUMBER string
# MAGIC ,SERVICE_KEY decimal(18,0)
# MAGIC ,CARRIER_LOS_KEY decimal(18,0)
# MAGIC ,ORIGIN_LOCATION_KEY decimal(18,0)
# MAGIC ,DESTINATION_LOCATION_KEY decimal(18,0)
# MAGIC ,CLIENT_KEY decimal(18,0)
# MAGIC ,ORDER_PLACED_DATE timestamp
# MAGIC ,TRANSACTION_TYPE_ID integer
# MAGIC ,IS_MANAGED integer
# MAGIC ,IS_ASN integer
# MAGIC ,SOURCE_ORDER_SUB_TYPE string
# MAGIC ,UTC_ORDER_PLACED_DATE timestamp
# MAGIC ,ORDER_LATEST_ACTIVITY_DATE timestamp
# MAGIC ,UTC_ORDER_LATEST_ACTIVITY_DATE timestamp
# MAGIC ,ORDER_CANCELLED_FLAG string
# MAGIC ,ORDER_CANCELLED_DATE timestamp
# MAGIC ,UTC_ORDER_CANCELLED_DATE timestamp
# MAGIC ,ORDER_SHIPPED_DATE timestamp
# MAGIC ,UTC_ORDER_SHIPPED_DATE timestamp
# MAGIC ,LOFST_ORDER_LATEST_ACTIVITY_DATE timestamp
# MAGIC ,SHIPMENT_COUNT integer
# MAGIC ,STO_ORDER_COUNT integer
# MAGIC ,ORDER_LATEST_ACTIVITY_DATE_KEY integer
# MAGIC ,SOURCE_ORDER_STATUS string
# MAGIC ,SOURCE_ORDER_TYPE string
# MAGIC ,FREIGHT_CARRIER_CODE string
# MAGIC ,WAYBILL_AIRBILL_NUM string
# MAGIC ,DONOT_SHIP_BEFORE_DATE timestamp
# MAGIC ,ORIGIN_TIME_ZONE string
# MAGIC ,DESTINATION_TIME_ZONE string
# MAGIC ,ORDER_SDUK string
# MAGIC ,ETL_BATCH_NUMBER decimal(18,0)
# MAGIC ,GLD_ACCOUNT_MAPPED_KEY string
# MAGIC ,DP_SERVICELINE_KEY string
# MAGIC ,DP_ORGENTITY_KEY string
# MAGIC ,EXT_CUSTOMER_ACCOUNT_NUMBER string
# MAGIC ,ServiceLevelName string
# MAGIC ,ServiceLevelCode string
# MAGIC ,CarrierCode string
# MAGIC ,CarrierName string
# MAGIC ,CARRIER_GROUP string
# MAGIC ,SERVICE_NAME_SR string
# MAGIC ,SERVICE_NAME_LC string
# MAGIC ,SERVICELEVELNAME_LC string
# MAGIC ,CARRIERNAME_LC string
# MAGIC ,FacilityId string
# MAGIC ,BUILDING_CODE string
# MAGIC ,WAREHOUSE_CODE string
# MAGIC ,ADDRESS_LINE_1 string
# MAGIC ,ADDRESS_LINE_2 string
# MAGIC ,CITY string
# MAGIC ,PROVINCE string
# MAGIC ,POSTAL_CODE string
# MAGIC ,COUNTRY string
# MAGIC ,WAREHOUSE_KEY_WSE decimal(18,0)
# MAGIC ,SOURCE_SYSTEM_NAME string
# MAGIC ,OrderStatusName string
# MAGIC ,TransactionTypeName string
# MAGIC ,ADDRESS_LINE_1_ORIGIN string
# MAGIC ,ADDRESS_LINE_2_ORIGIN string
# MAGIC ,CITY_ORIGIN string
# MAGIC ,PROVINCE_ORIGIN string
# MAGIC ,POSTAL_CODE_ORIGIN string
# MAGIC ,COUNTRY_ORIGIN string
# MAGIC ,LOCATION_CODE_ORIGIN string
# MAGIC ,ADDRESS_LINE_1_DESTINATION string
# MAGIC ,ADDRESS_LINE_2_DESTINATION string
# MAGIC ,CITY_DESTINATION string
# MAGIC ,PROVINCE_DESTINATION string
# MAGIC ,POSTAL_CODE_DESTINATION string
# MAGIC ,COUNTRY_DESTINATION string
# MAGIC ,LOCATION_CODE_DESTINATION string
# MAGIC ,LOCATION_NAME string
# MAGIC ,UTC_ORDER_PLACED_MONTH_part_key string
# MAGIC ,is_deleted int   --add this as well Mahesh
# MAGIC ,dl_hash	string
# MAGIC ,dl_insert_pipeline_id	string
# MAGIC ,dl_insert_timestamp	timestamp
# MAGIC ,dl_update_pipeline_id	string
# MAGIC ,dl_update_timestamp	timestamp
# MAGIC )
# MAGIC USING DELTA
# MAGIC partitioned by (UTC_ORDER_PLACED_MONTH_part_key)
# MAGIC LOCATION '/mnt/sail/gold/summary/fact_order_dim_inc'

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table fact_order_dim_inc

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_orders
# MAGIC (
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC UPSOrderNumber	string,
# MAGIC OrderNumber	string,
# MAGIC ReferenceOrder	string,
# MAGIC CustomerPO	string,
# MAGIC DateTimeReceived	timestamp,
# MAGIC UTC_DateTimeReceived	timestamp,
# MAGIC LatestStatusDate	timestamp,
# MAGIC UTC_LatestStatusDate	timestamp,
# MAGIC OrderCancelledFlag	string,
# MAGIC DateTimeCancelled	timestamp,
# MAGIC UTC_DateTimeCancelled	timestamp,
# MAGIC DateTimeShipped	timestamp,
# MAGIC UTC_DateTimeShipped	timestamp,
# MAGIC RouteCode	string,
# MAGIC ClientOrderType	string,
# MAGIC CancelledReasonCode	string,
# MAGIC LogiNext_OrderFlag	string,
# MAGIC LogiNext_OrderCurrentSegment	string,
# MAGIC OrderStatusName	string,
# MAGIC ServiceLevel	string,
# MAGIC Carrier	string,
# MAGIC ServiceMode	string,
# MAGIC ServiceLevelCode	string,
# MAGIC CarrierCode	string,
# MAGIC ConsigneeName	string,
# MAGIC OriginAddress1	string,
# MAGIC OriginAddress2	string,
# MAGIC OriginCity	string,
# MAGIC OriginProvince	string,
# MAGIC OriginPostalCode	string,
# MAGIC OriginCountry	string,
# MAGIC DestinationAddress1	string,
# MAGIC DestinationAddress2	string,
# MAGIC DestinationCity	string,
# MAGIC DestinationProvince	string,
# MAGIC DestinationPostalcode	string,
# MAGIC DestinationCountry	string,
# MAGIC OrderType	string,
# MAGIC SourceSystemKey	int,
# MAGIC SourceSystemName	string,
# MAGIC ShipmentCount	int,
# MAGIC IsSTO	int,
# MAGIC TrackingNo	string,
# MAGIC TodayFlag	string,
# MAGIC ORDER_REF_1_LABEL	string,
# MAGIC ORDER_REF_1_VALUE	string,
# MAGIC ORDER_REF_2_LABEL	string,
# MAGIC ORDER_REF_2_VALUE	string,
# MAGIC ORDER_REF_3_LABEL	string,
# MAGIC ORDER_REF_3_VALUE	string,
# MAGIC ORDER_REF_4_LABEL	string,
# MAGIC ORDER_REF_4_VALUE	string,
# MAGIC ORDER_REF_5_LABEL	string,
# MAGIC ORDER_REF_5_VALUE	string,
# MAGIC ORDER_REF_6_LABEL	string,
# MAGIC ORDER_REF_6_VALUE	string,
# MAGIC ORDER_REF_7_LABEL	string,
# MAGIC ORDER_REF_7_VALUE	string,
# MAGIC ORDER_REF_8_LABEL	string,
# MAGIC ORDER_REF_8_VALUE	string,
# MAGIC ORDER_REF_9_LABEL	string,
# MAGIC ORDER_REF_9_VALUE	string,
# MAGIC ORDER_REF_10_LABEL	string,
# MAGIC ORDER_REF_10_VALUE	string,
# MAGIC ORDER_REF_11_LABEL	string,
# MAGIC ORDER_REF_11_VALUE	string,
# MAGIC ORDER_REF_12_LABEL	string,
# MAGIC ORDER_REF_12_VALUE	string,
# MAGIC ORDER_REF_13_LABEL	string,
# MAGIC ORDER_REF_13_VALUE	string,
# MAGIC ORDER_REF_14_LABEL	string,
# MAGIC ORDER_REF_14_VALUE	string,
# MAGIC ORDER_REF_15_LABEL	string,
# MAGIC ORDER_REF_15_VALUE	string,
# MAGIC TransOnly	int,
# MAGIC TransMilestone	string,
# MAGIC ExceptionCode	string,
# MAGIC OriginalScheduledDeliveryDateTime	timestamp,
# MAGIC UTC_OriginalScheduledDeliveryDateTime	timestamp,
# MAGIC ActualScheduledDeliveryDateTime	timestamp,
# MAGIC UTC_ActualScheduledDeliveryDateTime	timestamp,
# MAGIC ActualDeliveryDate	timestamp,
# MAGIC UTC_ActualDeliveryDate	timestamp,
# MAGIC ScheduleShipmentDate	timestamp,
# MAGIC UTC_ScheduleShipmentDate	timestamp,
# MAGIC ActualShipmentDateTime	timestamp,
# MAGIC UTC_ActualShipmentDateTime	timestamp,
# MAGIC OrderWarehouse	string,
# MAGIC OrderLineCount	int,
# MAGIC OriginContactName	string,
# MAGIC TRANSPORT_MILESTONE_1	string,
# MAGIC TRANSPORT_MILESTONEDATE_1	timestamp,
# MAGIC TRANSPORT_MILESTONE_2	string,
# MAGIC TRANSPORT_MILESTONEDATE_2	timestamp,
# MAGIC TRANSPORT_MILESTONE_3	string,
# MAGIC TRANSPORT_MILESTONEDATE_3	timestamp,
# MAGIC TRANSPORT_MILESTONE_4	string,
# MAGIC TRANSPORT_MILESTONEDATE_4	timestamp,
# MAGIC TRANSPORT_MILESTONE_5	string,
# MAGIC TRANSPORT_MILESTONEDATE_5	timestamp,
# MAGIC TRANSPORT_MILESTONE_6	string,
# MAGIC TRANSPORT_MILESTONEDATE_6	timestamp,
# MAGIC SOURCE_ORDER_STATUS	string,
# MAGIC TRANS_MILESTONE	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC LOAD_ID	string,
# MAGIC UPSTransportShipmentNumber	string,
# MAGIC CurrentMilestone	string,
# MAGIC CurrentMilestoneDate	timestamp,
# MAGIC IS_INBOUND	int,
# MAGIC SHIPMENT_NOTES	string,
# MAGIC IS_ASN	int,
# MAGIC TransactionTypeName	string,
# MAGIC ShipmentBookedDate	timestamp,
# MAGIC GFF_ShipmentNumber	string,
# MAGIC GFF_ShipmentInstanceId	string,
# MAGIC Freight_Carriercode	string,
# MAGIC WAYBILL_AIRBILL_NUM	string,
# MAGIC PROOF_OF_DELIVERY_NAME	string,
# MAGIC ActualScheduledDeliveryDateTimeZone	string,
# MAGIC ShippedDateTimeZone	string,
# MAGIC EquipmentType	string,
# MAGIC OriginTimeZone	string,
# MAGIC DestinationTimeZone	string,
# MAGIC DestinationLocationCode	string,
# MAGIC OriginLocationCode	string,
# MAGIC AuthorizerName	string,
# MAGIC DeliveryInstructions	string,
# MAGIC DestinationContactName	string,
# MAGIC PickUPDateTime	timestamp,
# MAGIC ScheduledPickUpDateTime	timestamp,
# MAGIC RowNumber	int,
# MAGIC BATCH_ID	string,
# MAGIC Account_number	int,
# MAGIC EstimatedDeliveryDateTime	timestamp,
# MAGIC ActualDeliveryDateTime	timestamp,
# MAGIC ORDER_SDUK	string,
# MAGIC is_healthcare int,
# MAGIC is_deleted	int,
# MAGIC UTC_ORDER_PLACED_MONTH_part_key string,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (UTC_ORDER_PLACED_MONTH_part_key)
# MAGIC location "/mnt/sail/gold/summary/digital_summary_orders"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_milestone_activity
# MAGIC (
# MAGIC SourceSystemKey	int,
# MAGIC SourceSystemName	string,
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC UPSOrderNumber	string,
# MAGIC TrackingNumber	string,
# MAGIC Source_Activity_Code	string,
# MAGIC Source_Activity_Name	string,
# MAGIC TransactionTypeId	int,
# MAGIC MilestoneId	int,
# MAGIC MilestoneName	string,
# MAGIC ActivityId	int,
# MAGIC ActivityCode	string,
# MAGIC ActivityName	string,
# MAGIC ActivityDate	timestamp,
# MAGIC ActivityCompletionFlag	string,
# MAGIC PlannedMilestoneDate	timestamp,
# MAGIC MilestoneDate	timestamp,
# MAGIC MilestoneCompletionFlag	string,
# MAGIC MilestoneOrder	int,
# MAGIC CurrentMilestoneFlag	string,
# MAGIC UPSASNNumber	string,
# MAGIC SEGMENT_ID	int,
# MAGIC ACTIVITY_NOTES	string,
# MAGIC VENDOR_NAME	string,
# MAGIC PROOF_OF_DELIVERY_NAME	string,
# MAGIC CARRIER_TYPE	string,
# MAGIC P_Flag	int,
# MAGIC LOAD_TRACK_SDUK	string,
# MAGIC FTZ_STATUS	string,
# MAGIC TimeZone	string,
# MAGIC LOGI_NEXT_FLAG	string,
# MAGIC BATCH_ID	int,
# MAGIC ACTIVITY_MONTH_part_key string,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC PARTITIONED BY (ACTIVITY_MONTH_part_key) 
# MAGIC location "/mnt/sail/gold/summary/digital_summary_milestone_activity"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_milestone_activity

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_transportation_callcheck
# MAGIC (
# MAGIC UPSORDERNUMBER	string,
# MAGIC SOURCESYSTEMKEY	int,
# MAGIC ACCOUNTID	string,
# MAGIC FACILITYID	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC LOADID	string,
# MAGIC LATEST_TEMPERATURE	string,
# MAGIC TEMPERATURE_CITY	string,
# MAGIC TEMPERATURE_STATE	string,
# MAGIC TEMPERATURE_COUNTRY	string,
# MAGIC IS_TEMPERATURE	string,
# MAGIC TEMPERATURE_DATETIME	timestamp,
# MAGIC ACTIVITYTYPE	string,
# MAGIC STATUSDETAILTYPE	string,
# MAGIC IS_LATEST_TEMPERATURE	string,
# MAGIC TemperatureC	decimal(18,0),
# MAGIC TemperatureF	decimal(18,0),
# MAGIC BatteryPercent	int,
# MAGIC Humidity	int,
# MAGIC Light	decimal(18,0),
# MAGIC IsShockExceeded	boolean,
# MAGIC Latitude	decimal(18,0),
# MAGIC Longitude	decimal(18,0),
# MAGIC DeviceTagId	string,
# MAGIC LocationMethod	string,
# MAGIC IsMotionDetected	boolean,
# MAGIC Pressure	decimal(18,0),
# MAGIC IsButtonPushed	boolean,
# MAGIC order_sduk	string,
# MAGIC transportation_sduk	string,
# MAGIC CALLCHECK_SDUK	string,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transportation_callcheck"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_transportation_callcheck

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_transportation_references
# MAGIC (
# MAGIC UPSOrderNumber	string,
# MAGIC SourceSystemKey	int,
# MAGIC LOAD_ID	string,
# MAGIC ShipUnitId	string,
# MAGIC ReferenceType	string,
# MAGIC ReferenceValue	string,
# MAGIC ReferenceLevel	string,
# MAGIC REFERENCE_SDUK	string,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transportation_references"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_transportation_references

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_transportation_rates_charges
# MAGIC (
# MAGIC UpsOrderNumber	string,
# MAGIC SourceSystemKey	int,
# MAGIC load_id	string,
# MAGIC SequenceNumber	string,
# MAGIC ChargeType	string,
# MAGIC Rate	string,
# MAGIC RateQualifer	string,
# MAGIC Charge	string,
# MAGIC ChargeDescription	string,
# MAGIC ChargeLevel	string,
# MAGIC EdiCode	string,
# MAGIC FreightClass	string,
# MAGIC FAKFreightClass	string,
# MAGIC ContractName	string,
# MAGIC CurrencyCode	string,
# MAGIC InvoiceNumber	string,
# MAGIC order_sduk string,
# MAGIC charge_sduk	string,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transportation_rates_charges"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_transportation_rates_charges

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_order_tracking
# MAGIC (
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC UPSOrderNumber	string,
# MAGIC SourceSystemKey	int,
# MAGIC ShipmentDimensions	string,
# MAGIC ShipmentWeight	string,
# MAGIC CarrierCode	string,
# MAGIC TRACKING_NUMBER	string,
# MAGIC CarrierType	string,
# MAGIC ShipmentDimensions_UOM	string,
# MAGIC ShipmentWeight_UOM	string,
# MAGIC TemperatureRange_Min	string,
# MAGIC TemperatureRange_Max	string,
# MAGIC TemperatureRange_UOM	string,
# MAGIC TemperatureRange_Code	string,
# MAGIC SHIPMENT_QUANTITY	decimal(18,0),
# MAGIC SHIPMENT_DESCRIPTION	string,
# MAGIC LOAD_AREA	decimal(18,0),
# MAGIC UOM	string,
# MAGIC order_sduk	string,
# MAGIC SHIPMENT_SDUK	string,
# MAGIC UTC_SHIPMENT_CREATION_MONTH_PART_KEY bigint,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (UTC_SHIPMENT_CREATION_MONTH_PART_KEY)
# MAGIC location "/mnt/sail/gold/summary/digital_summary_order_tracking"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_order_tracking

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_transportation
# MAGIC (
# MAGIC UpsOrderNumber	string,
# MAGIC SourceSystemKey	int,
# MAGIC UpsWMSOrderNumber	string,
# MAGIC UpsWMSSourceSystemKey	int,
# MAGIC SourceOrderType	string,
# MAGIC EquipmentType	string,
# MAGIC SourceOrderSubType	string,
# MAGIC OriginCompany	string,
# MAGIC DestinationCompany	string,
# MAGIC OriginTimeZone	string,
# MAGIC DestinationTimeZone	string,
# MAGIC SourceOrderState	string,
# MAGIC SourceOrderStatus	string,
# MAGIC OrderCancelledFlag	string,
# MAGIC Order_Rec_CreatedDate	timestamp,
# MAGIC UTC_Order_Rec_CreatedDate	timestamp,
# MAGIC LOFST_Rec_CreatedDate	timestamp,
# MAGIC OrderPlacedDate	timestamp,
# MAGIC UTC_OrderPlacedDate	timestamp,
# MAGIC LOFST_OrderPlacedDate	timestamp,
# MAGIC OrderCancelledDate	timestamp,
# MAGIC UTC_OrderCancelledDate	timestamp,
# MAGIC LOFST_OrderCancelledDate	timestamp,
# MAGIC OrderShippedDate	timestamp,
# MAGIC UTC_OrderShippedDate	timestamp,
# MAGIC LOFST_OrderShippedDate	timestamp,
# MAGIC ScheduledShipmentDate	timestamp,
# MAGIC UTC_ScheduledShipmentDate	timestamp,
# MAGIC LOFST_ScheduledShipmentDate	timestamp,
# MAGIC ActualShipmentDate	timestamp,
# MAGIC UTC_ActualShipmentDate	timestamp,
# MAGIC LOFST_ActualShipmentDate	timestamp,
# MAGIC ScheduledDeliveryDate	timestamp,
# MAGIC UTC_ScheduledDeliveryDate	timestamp,
# MAGIC LOFST_ScheduledDeliveryDate	timestamp,
# MAGIC ActualDeliveryDate	timestamp,
# MAGIC UTC_ActualDeliveryDate	timestamp,
# MAGIC LOFST_ActualDeliveryDate	timestamp,
# MAGIC OrderCount	int,
# MAGIC OriginalScheduledDeliveryDate	timestamp,
# MAGIC UTC_OriginalScheduledDeliveryDate	timestamp,
# MAGIC LOFST_OriginalScheduledDeliveryDate	timestamp,
# MAGIC LOAD_ID	string,
# MAGIC LoadEarliestPickUpDate	timestamp,
# MAGIC LoadLatestPickUpDate	timestamp,
# MAGIC LoadEarliestDeliveryDate	timestamp,
# MAGIC LoadLatestDeliveryDate	timestamp,
# MAGIC LoadCreationDate	timestamp,
# MAGIC LoadUpdateDate	timestamp,
# MAGIC CarrierCode	string,
# MAGIC LevelOfServiceCode	string,
# MAGIC WMSPONumber	string,
# MAGIC CarrierMode	string,
# MAGIC TrasOnlyFlag	string,
# MAGIC ShipmentNotes	string,
# MAGIC Comments	string,
# MAGIC GFFShipmentNumber	string,
# MAGIC GFFShipmentInstanceNumber	string,
# MAGIC ProofOfDelivery	string,
# MAGIC Scope	string,
# MAGIC Sector	string,
# MAGIC Direction	string,
# MAGIC AuthorizerName	string,
# MAGIC DeliveryInstructions	string,
# MAGIC DestinationContact	string,
# MAGIC order_sduk	string,
# MAGIC transportation_sduk	string,
# MAGIC UTC_ORDER_PLACED_MONTH_PART_KEY bigint,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC PARTITIONED BY (UTC_ORDER_PLACED_MONTH_PART_KEY) 
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transportation"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_transportation

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_order_lines
# MAGIC (
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC UPSOrderNumber	string,
# MAGIC OrderNumber	string,
# MAGIC LineNUmber	string,
# MAGIC SKU	string,
# MAGIC SKUDescription	string,
# MAGIC SKUDimensions	string,
# MAGIC SKUWeight	decimal(18,0),
# MAGIC SKUQuantity	decimal(18,0),
# MAGIC SKUShippedQuantity	decimal(18,0),
# MAGIC CarrierCode	string,
# MAGIC TrackingNo	string,
# MAGIC SourceSystemKey	int,
# MAGIC SKUDimensions_UOM	string,
# MAGIC SKUWeight_UOM	string,
# MAGIC ShipmentLineCanceledDate	timestamp,
# MAGIC ShipmentLineCanceledReason	string,
# MAGIC ShipmentLineCanceledBy	string,
# MAGIC ShipmentLineCanceledFlag	string,
# MAGIC LineRefVal1	string,
# MAGIC LineRefVal2	string,
# MAGIC LineRefVal3	string,
# MAGIC LineRefVal4	string,
# MAGIC LineRefVal5	string,
# MAGIC order_sduk	string,
# MAGIC order_line_sduk	string,
# MAGIC UTC_ORDER_PLACED_MONTH_PART_KEY bigint,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (UTC_ORDER_PLACED_MONTH_PART_KEY)
# MAGIC location "/mnt/sail/gold/summary/digital_summary_order_lines"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_order_lines

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_order_lines_details
# MAGIC (
# MAGIC AccountId	string,
# MAGIC FacilityId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC UPSOrderNumber	string,
# MAGIC LineNumber	string,
# MAGIC LineDetailNumber	string,
# MAGIC VendorSerialNumber	string,
# MAGIC VendorLotNumber	string,
# MAGIC LPNNumber	string,
# MAGIC DispositionValue	string,
# MAGIC SourceSystemKey	int,
# MAGIC WarehouseKey	decimal(18,0),
# MAGIC ItemKey	decimal(18,0),
# MAGIC itemNumber	string,
# MAGIC EXPIRATION_DATE	timestamp,
# MAGIC WAREHOUSE_CODE	string,
# MAGIC order_sduk	string,
# MAGIC ORDER_LINE_DETAILS_SDUK	string,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_order_lines_details"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_order_lines_details

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_milestone
# MAGIC (
# MAGIC UPSOrderNumber	string,
# MAGIC SourceSystemKey	int,
# MAGIC SourceSystemName	string,
# MAGIC AccountId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC FacilityId	string,
# MAGIC WarehouseCode	string,
# MAGIC TransactionTypeName	string,
# MAGIC MilestoneName	string,
# MAGIC MilestoneOrder	int,
# MAGIC order_sduk	string,
# MAGIC UTC_ORDER_PLACED_MONTH_part_key string,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC partitioned by (UTC_ORDER_PLACED_MONTH_part_key)
# MAGIC location "/mnt/sail/gold/summary/digital_summary_milestone"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_milestone

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_exceptions
# MAGIC (
# MAGIC UPSOrderNumber	string,
# MAGIC SourceSystemKey	int,
# MAGIC UTC_ExceptionCreatedDate	timestamp,
# MAGIC OTZ_ExceptionCreatedDate	timestamp,
# MAGIC LOFST_ExceptionCreatedDate_OTZ	timestamp,
# MAGIC ExceptionCreatedDate_DTZ	timestamp,
# MAGIC LOFST_ExceptionCreatedDate_DTZ	timestamp,
# MAGIC ExceptionDescription	string,
# MAGIC ExceptionEvent	string,
# MAGIC ExceptionReason	string,
# MAGIC ExceptionReasonType	string,
# MAGIC ExceptionCategory	string,
# MAGIC ResponsibleParty	string,
# MAGIC ExceptionPrimaryIndicator	decimal(18,0),
# MAGIC ExceptionCount	int,
# MAGIC TRANSPORTATION_EXCEPTION_SDUK	string,
# MAGIC ExceptionType	string,
# MAGIC DateTimeShippedTimeZone	string,
# MAGIC ActualScheduledDeliveryDateTimeZone	string,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_exceptions"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_exceptions

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_transport_details
# MAGIC (
# MAGIC UPSORDERNUMBER	string,
# MAGIC SOURCE_SYSTEM_KEY	int,
# MAGIC Account_ID	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC ITEM_DESCRIPTION	string,
# MAGIC ACTUAL_QTY	decimal(18,0),
# MAGIC ACTUAL_UOM	string,
# MAGIC ACTUAL_WGT	string,
# MAGIC ITEM_DIMENSION	string,
# MAGIC TempRangeMin	string,
# MAGIC TempRangeMax	string,
# MAGIC TempRangeUOM	string,
# MAGIC TempRangeCode	string,
# MAGIC PlannedWeightUOM	string,
# MAGIC ActualWeightUOM	string,
# MAGIC DimensionUOM	string,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_transport_details"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_transport_details

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_inbound_line
# MAGIC (
# MAGIC AccountId	string,
# MAGIC DP_SERVICELINE_KEY	string,
# MAGIC DP_ORGENTITY_KEY	string,
# MAGIC FacilityId	string,
# MAGIC FacilityCode	string,
# MAGIC UPSOrderNumber	string,
# MAGIC UPSASNNumber	string,
# MAGIC ClientASNNumber	string,
# MAGIC ClientPONumber	string,
# MAGIC ReceiptNumber	string,
# MAGIC ReceiptLineNumber	string,
# MAGIC ShippedQuantity	decimal(18,0),
# MAGIC ReceivedQuantity	decimal(18,0),
# MAGIC CreationDateTime	timestamp,
# MAGIC SKU	string,
# MAGIC SKUDescription	string,
# MAGIC SKUDimensions	string,
# MAGIC SKUWeight	decimal(18,0),
# MAGIC SKUDimensions_UOM	string,
# MAGIC SKUWeight_UOM	string,
# MAGIC SourceSystemKey	int,
# MAGIC InboundLine_Reference2	string,
# MAGIC InboundLine_Reference10	string,
# MAGIC InboundLine_Reference11	string,
# MAGIC PutAwayDate	timestamp,
# MAGIC is_deleted	int,
# MAGIC hash_key	string,
# MAGIC dl_hash	string,
# MAGIC dl_insert_pipeline_id	string,
# MAGIC dl_insert_timestamp	timestamp,
# MAGIC dl_update_pipeline_id	string,
# MAGIC dl_update_timestamp	timestamp
# MAGIC )
# MAGIC using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_inbound_line"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_inbound_line

# COMMAND ----------

# MAGIC %sql
# MAGIC create external table digital_summary_inventory
# MAGIC (SourceSystemKey	int
# MAGIC ,SourceSystemName	string
# MAGIC ,AccountId	string
# MAGIC ,FacilityId	string
# MAGIC ,itemNumber	string
# MAGIC ,itemDescription	string
# MAGIC ,hazardClass	string
# MAGIC ,itemDimensions_length	decimal(22,4)
# MAGIC ,itemDimensions_width	decimal(22,4)
# MAGIC ,itemDimensions_height	decimal(22,4)
# MAGIC ,itemDimensions_unitOfMeasurement_code	string
# MAGIC ,itemWeight_weight	decimal(22,4)
# MAGIC ,itemWeight_unitOfMeasurement_Code	string
# MAGIC ,warehouseCode	string
# MAGIC ,availableQuantity	decimal(32,4)
# MAGIC ,nonAvailableQuantity	decimal(32,4)
# MAGIC ,DP_SERVICELINE_KEY	string
# MAGIC ,DP_ORGENTITY_KEY	string
# MAGIC ,InvRef1	string
# MAGIC ,InvRef2	string
# MAGIC ,InvRef3	string
# MAGIC ,InvRef4	string
# MAGIC ,InvRef5	string
# MAGIC ,LPNNumber	string
# MAGIC ,HazmatClass	string
# MAGIC ,StrategicGoodsFlag	string
# MAGIC ,UNNumber	string
# MAGIC ,Designator	string
# MAGIC ,VendorSerialNumber	string
# MAGIC ,VendorLotNumber	string
# MAGIC ,BatchStatus	string
# MAGIC ,ExpirationDate	timestamp
# MAGIC ,Account_number	string
# MAGIC ,BatchHoldReason	string
# MAGIC ,HoldDescription	string
# MAGIC ,is_deleted	int
# MAGIC ,hash_key string
# MAGIC ,dl_hash	string
# MAGIC ,dl_insert_pipeline_id	string
# MAGIC ,dl_insert_timestamp	string
# MAGIC ,dl_update_pipeline_id	string
# MAGIC ,dl_update_timestamp	string
# MAGIC ) using delta
# MAGIC location "/mnt/sail/gold/summary/digital_summary_inventory"

# COMMAND ----------

# MAGIC %sql drop table digital_summary_inventory

# COMMAND ----------

# %sql
# create external table digital_summary_onboarded_systems
# (
# sourcesystemkey int,
# sourcesystemname string,
# dl_insert_timestamp	timestamp
# )
# using delta
# location "/mnt/sail/logs/controller.db/digital_summary_onboarded_systems"

# COMMAND ----------

# %sql drop table digital_summary_onboarded_systems

# COMMAND ----------

# %sql insert into delta.`/mnt/sail/logs/controller.db/digital_summary_onboarded_systems`
# values (1002,'SPLUS',current_timestamp)

# COMMAND ----------

# MAGIC %sql select * from delta.`/mnt/sail/logs/controller.db/digital_summary_onboarded_systems`-- where is_deleted =1