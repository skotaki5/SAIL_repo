# Databricks notebook source
"""
Author           : Vinoth Kumar Gopal
Description      : this notebook is to load inbound digital_summary_transportation_rates_charges cosmos container.
"""

# COMMAND ----------

# DBTITLE 1,Importing python libraries
import datetime
from pyspark.sql.types import StringType, ArrayType, StructType, StructField, DecimalType
from pyspark.sql.functions import col, from_json

# COMMAND ----------

# DBTITLE 1,Importing common variables
# MAGIC %run "/SAIL/includes/common_variables"

# COMMAND ----------

# DBTITLE 1,Importing common udfs
# MAGIC %run "/SAIL/includes/common_udfs"

# COMMAND ----------

# DBTITLE 1,Spark Configs
spark.conf.set("spark.databricks.io.cache.enabled","true")

# COMMAND ----------

# DBTITLE 1,Cosmos connection
#Cosmos connection
scope = 'key-vault-secrets'
cosmosEndpoint = dbutils.secrets.get(scope,"cosmosEndpoint")
cosmosMasterKey = dbutils.secrets.get(scope,"cosmosMasterKey")
cosmosDatabaseName = "SAIL"
cosmosContainerName = "digital_summary_transportation_rates_charges"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName
}



# COMMAND ----------

st_dt =datetime.now(tz=timezone(time_zone))
start_time = st_dt.strftime("%Y-%m-%d %H:%M:%S")

# COMMAND ----------

# DBTITLE 1,Setting debug mode
dbutils.widgets.text("log_debug_mode", "","")
dbutils.widgets.get("log_debug_mode")
log_debug_mode = getArgument("log_debug_mode").strip()

if log_debug_mode == "Y":
  logger = _get_logger('US/Eastern',logging.DEBUG)  #UTC timezone
else:
  logger = _get_logger('US/Eastern',logging.INFO)

# COMMAND ----------

def get_delta_query(hwm):
  logger.debug("hwm: " + str(hwm))
  query ="""
        create or replace temp view rates_charges_delta_fetch_tv 
        as
        select RC_O.* from {digital_summary_transportation_rates_charges} RC_O
        inner join (select distinct UpsOrderNumber from {digital_summary_transportation_rates_charges} RC_IN 
        inner join {digital_summary_onboarded_systems} OS on  RC_IN.SourceSystemKey = OS.sourcesystemkey  -- (select '1011' as sourcesystemkey) 
        where RC_IN.dl_update_timestamp>='{hwm}') dt ON RC_O.UpsOrderNumber = dt.UpsOrderNumber
        """.format(**source_tables,hwm=hwm)
  logger.debug("query : " + query)
  return(query)

# COMMAND ----------

def get_pre_cosmos_query():
  query = """
  select 
md5(concat(nvl(UpsOrderNumber,''),nvl(is_inbound,''),nvl(AccountId,''),nvl(DP_SERVICELINE_KEY,''),nvl(DateTimeReceived,''))) as id,
UpsOrderNumber, 
is_inbound,
AccountId,
DP_SERVICELINE_KEY,
DateTimeReceived AS invoiceDateTime,
sum(totalCustomerCharge) as totalCustomerCharge,
max(totalCustomerChargeCurrency) as totalCustomerChargeCurrency,
collect_list(val) AS CostBreakdown
from
(select UpsOrderNumber,named_struct('CostBreakdownType',ChargeDescription,
'CurrencyCode',CurrencyCode,
'CostBreakdownValue',sum(cast(charge as decimal(10,2)))) as val,
sum(CAST(Charge as decimal(10,2))) AS totalCustomerCharge,
MAX(CurrencyCode) AS totalCustomerChargeCurrency,
is_inbound,
AccountId,
DP_SERVICELINE_KEY,
DateTimeReceived
from
(select 
O.UpsOrderNumber,O.SourceSystemKey,TRC.ChargeDescription,TRC.CurrencyCode,TRC.charge,
TRC.CurrencyCode,O.is_inbound,O.AccountId,O.DP_SERVICELINE_KEY,O.DateTimeReceived
from {rates_charges_delta_fetch_tv} TRC
inner join (select distinct UPSOrderNumber,UPSTransportShipmentNumber,SourceSystemKey,is_inbound,AccountId,DP_SERVICELINE_KEY,DateTimeReceived 
  from {digital_summary_orders} DSO
  where is_inbound = '0'
  and DateTimeReceived>=current_date-{days_back}
  --and UPSOrderNumber='3241186'
  ) O 
  on TRC.UPSOrderNumber = O.UPSTransportShipmentNumber 
  where is_deleted= 0 and ChargeLevel = 'CUSTOMER_RATES'
  ) inn
  group by UpsOrderNumber,is_inbound,AccountId,DP_SERVICELINE_KEY,DateTimeReceived,ChargeDescription,CurrencyCode) 
group by UpsOrderNumber,is_inbound,AccountId,DP_SERVICELINE_KEY,DateTimeReceived
union
select 
md5(concat(nvl(UpsOrderNumber,''),nvl(is_inbound,''),nvl(AccountId,''),nvl(DP_SERVICELINE_KEY,''),nvl(DateTimeReceived,''))) as id,
UpsOrderNumber, 
is_inbound,
AccountId,
DP_SERVICELINE_KEY,
DateTimeReceived AS invoiceDateTime,
sum(totalCustomerCharge) as totalCustomerCharge,
max(totalCustomerChargeCurrency) as totalCustomerChargeCurrency,
collect_list(val) AS CostBreakdown
from
(select UpsOrderNumber,named_struct('CostBreakdownType',ChargeDescription,
'CurrencyCode',CurrencyCode,
'CostBreakdownValue',sum(cast(charge as decimal(10,2)))) as val,
sum(CAST(Charge as decimal(10,2))) AS totalCustomerCharge,
MAX(CurrencyCode) AS totalCustomerChargeCurrency,
is_inbound,
AccountId,
DP_SERVICELINE_KEY,
DateTimeReceived
from
(select 
O.UpsOrderNumber,O.SourceSystemKey,TRC.ChargeDescription,TRC.CurrencyCode,TRC.charge,
TRC.CurrencyCode,O.is_inbound,O.AccountId,O.DP_SERVICELINE_KEY,O.DateTimeReceived
from {rates_charges_delta_fetch_tv} TRC
inner join (select distinct UPSOrderNumber,SourceSystemKey,is_inbound,AccountId,DP_SERVICELINE_KEY,DateTimeReceived 
  from {digital_summary_orders} DSO
  where is_inbound in ('1','2')
  and DateTimeReceived>=current_date-{days_back}
  --and UPSOrderNumber='SWSN8282239'
  ) O 
  on TRC.UPSOrderNumber = O.UPSOrderNumber 
  where is_deleted= 0 and ChargeLevel = 'CUSTOMER_RATES'
  ) inn
  group by UpsOrderNumber,is_inbound,AccountId,DP_SERVICELINE_KEY,DateTimeReceived,ChargeDescription,CurrencyCode) 
group by UpsOrderNumber,is_inbound,AccountId,DP_SERVICELINE_KEY,DateTimeReceived
  """.format(**source_tables, rates_charges_delta_fetch_tv='rates_charges_delta_fetch_tv',days_back=days_back)
  return (query)

# COMMAND ----------

# DBTITLE 1,Main function
def main():
  logger.info('Main function is running')
  
  audit_result['process_name'] = 'load_digital_summary_transportation_rates_charges_Container'
  audit_result['process_type'] = 'DataBricks'
  audit_result['layer'] = 'cosmos'
  audit_result['table_name'] = 'cosmos_digital_summary_transportation_rates_charges'
  audit_result['process_date'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d")
  audit_result['start_time'] = start_time
  
  try:
      
    pid_get = get_pid()
    logger.debug("pid_get: {pid_get}".format(pid_get=pid_get))
    pid =  datetime.now(tz=timezone(time_zone)).strftime("%Y%m%d%H%M%S") if pid_get == '-1|-1' else pid_get
    logger.info("pid: {pid}".format(pid=pid))
    
    audit_result['process_id'] = pid
    
    hwm=get_hwm('cosmos','cosmos_digital_summary_transportation_rates_charges')
#     if hwm=='1900-01-01 00:00:00':
#       d = timedelta(days = 90)
#       back_date=st_dt - d
#       hwm=back_date.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f'hwm cosmos_digital_summary_transportation_rates_charges: {hwm}'.format(hwm=hwm))
    
    logger.info("Creating digital summary transportation rates charges views for incremental data")
    spark.sql(get_delta_query(hwm))
    logger.info("get_delta_query finished")  
    
    logger.info('Reading source data...')
    
    src_query =get_pre_cosmos_query()
    logger.debug('cosmos_query : ' + src_query)
    
    cosmos_df = spark.sql(src_query)
    
    logger.debug("Adding audit columns")
    cosmos_df = add_audit_columns(cosmos_df, pid,datetime.now(),datetime.now())
    cnt=cosmos_df.count()
    logger.info('count is {cnt}'.format(cnt=cnt))
    
    logger.info('Writing to Cosmos: {container_name}'.format(container_name=cosmosContainerName))
    cosmos_df.write.format("cosmos.oltp").options(**cfg).mode("APPEND").save()
    
    logger.info('setting hwm')
    res=set_hwm('cosmos','cosmos_digital_summary_transportation_rates_charges',start_time,pid)
    logger.info(res)
      
      
    audit_result['numTargetRowsInserted'] = cnt
    audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
    audit_result['status'] = 'success'   
    logger.info('Process finished successfully.')
    
  except Exception as e:
    audit_result['status'] = 'failed'
    audit_result['end_time'] = datetime.now(tz=timezone(time_zone)).strftime("%Y-%m-%d %H:%M:%S")
    audit_result['ERROR_MESSAGE'] = str(e)
    
    raise
  finally:
    logger.info("audit_result: {audit_result}".format(audit_result=audit_result))
    audit(audit_result)

# COMMAND ----------

main()