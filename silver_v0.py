import sys
import os
from pyspark.sql.functions import col, coalesce, lit, to_date, substring
from pyspark.sql import DataFrame
import json
from datetime import datetime, timezone
from diem.ctx import dbutils
from typing import Any, cast
from pyspark.sql import SparkSession

total_record_count_input = 0
total_record_processed_transaction = 0
total_record_transaction_input = 0

duration = 0
md_batch_id = dbutils.jobs.taskValues.get(taskKey="bronze", key="md_batch_id")



    return (spark.createDataFrame(mapping_data, 
                                       schema=["pos", "modality",          
                                                "fulfillment_method", 
                                                "terminal_modality",
                                                "business_partnership"]))

def flatten_transaction_level(silver_df: DataFrame) -> DataFrame:

    #create expression to apply mapping rule
    mapping_rules = [
    (("ACE", "INSTORE", "SCANFULLSVC"), "STAFF_LANE", "KROGER"),
    (("BRAVO", "INSTORE", "SCANFULLSVC"), "STAFF_LANE", "KROGER"),
    (("ACE", "INSTORE", "SCANSELF"), "SELF_CHECKOUT", "KROGER"),
    (("ACE", "INSTORE", "FUEL_KIOSK"), "FUEL_KIOSK", "KROGER"),
    (("ACE", "INSTORE", "FUELPUMP"), "FUEL_PUMP", "KROGER"),
    (("ACE", "INSTORE", "PHARMACYONLINE"), "PHARMACY", "EPRN_ONLINEPAY"),
    (("ACE", "INSTORE", "PHARMACYONLINE"), "PHARMACY", "PHARMACYONLINE"),
    (("EPOS", "INSTORE", "STARBUCKS"), "OTHER_INSTORE", "STARBUCKS"),
    (("CAPER", "INSTORE", "SMARTCART"), "SMART_CART", "CAPER"),
    (("ACE", "PICKUP", "INSTACARTMARKETPLACE"), "STAFF_LANE", "INSTACART_MARKETPLACE"),
    (("EPOS", "PICKUP", "CURBSIDE"), "OTHER_INSTORE", "KROGER"),
    (("EPOS", "PICKUP", "ONLINE"), "OTHER_INSTORE", "ALASKA_BUSH"),
    (("ACE", "DELIVERY", "DOORDASHMARKETPLACE"), "STAFF_LANE", "DOORDASH_MARKETPLACE"),
    (("ACE", "DELIVERY", "DOORDASHFRESHREADY"), "STAFF_LANE", "DOORDASH_FRESHREADY"),
    (("ACE", "DELIVERY", "INSTACARTMARKETPLACE"), "STAFF_LANE", "INSTACART_MARKETPLACE"),
    (("ACE", "DELIVERY", "INSTACARTMARKETPLACE_DELIVERY_NOW"), "STAFF_LANE", "INSTACART_MARKETPLACE_DELIVERY_NOW"),
    (("ACE", "DELIVERY", "INSTACARTFRESHREADY"), "STAFF_LANE", "INSTACART_FRESHREADY"),
    (("ACE", "DELIVERY", "INSTACARTPBI"), "STAFF_LANE", "INSTACART_PBI"),
    (("ACE", "DELIVERY", "INSTACARTSEAMLESS"), "STAFF_LANE", "INSTACART_SEAMLESS"),
    (("ACE", "DELIVERY", "INSTACARTSEAMLESS_EXPEDITED"), "STAFF_LANE", "INSTACART_SEAMLESS_EXPEDITED"),
    (("ACE", "DELIVERY", "INSTACARTSEAMLESS_EXPRESS"), "STAFF_LANE", "INSTACART_SEAMLESS_EXPRESS"),
    (("EPOS", "DELIVERY", "ONLINE"), "OTHER_INSTORE", "KROGER"),
    (("EPOS", "DELIVERY", "OTHERDELIVERY"), "OTHER_INSTORE", "UBER_EATS"),
    (("EPOS", "DELIVERY", "OTHERDELIVERY_UBER"), "OTHER_INSTORE", "UBER_EATS"),
    (("EPOS", "DELIVERY", "OTHERDELIVERY_DOORDASH"), "OTHER_INSTORE", "DOORDASH"),
    (("EPOS", "DELIVERY", "SHIPT"), "OTHER_INSTORE", "SHIPT"),
    (("EPOS", "DELIVERY", "PBE"), "OTHER_INSTORE", "KROGER_BOOST"),
    ]

    # Build expressions for terminal_modality and business_partnership
    expr1 = when(col("terminal_modality").isNotNull(), col("terminal_modality"))

    expr2 = when(col("business_partnership").isNotNull(), col("business_partnership"))

    for (pos, modality, fulfillment_method), terminal_modality, business_partnership in mapping_rules:
        condition = (
            (col("pos") == pos) &
            (col("modality") == modality) &
            (col("fulfillment_method") == fulfillment_method)
        )
        expr1 = expr1.when(condition, lit(terminal_modality))
        expr2 = expr2.when(condition, lit(business_partnership))

    ftt_transaction = silver_df.select(
        col("transaction_id"),
        col("transaction_id_gen"),
        col("md_batch_id"),
        coalesce(col("transactions.transactionInfo.locationId"), col("locationId"))
        .cast("string").alias("location_id"),
        coalesce(col("transactions.transactionInfo.transactionDateTime"), 
                 col("transactionDateTime")).alias("transaction_date_time_UTC"),
        coalesce(col("transactions.transactionInfo.transactionDateTimeLocal"), 
                 col("transactionDateTimeLocal")).alias("transaction_date_time_local"),
        coalesce(col("transactions.transactionInfo.transactionType"), col("transactionType"))
        .cast("string").alias("transaction_type"),
        coalesce(col("transactions.transactionInfo.operatorId"), col("operatorId"))
        .cast("string").alias("operator_id"),
        coalesce(col("transactions.transactionInfo.customerId"), col("customerId"))
        .cast("string").alias("customer_id"),
        col("transactions.businessDate").cast("date").alias("business_date"),
        coalesce(col("transactions.transactionInfo.krogerOrderNo"), col("krogerOrderNo"))
        .cast("string").alias("kroger_order_no"),
        col("transactions.transactionInfo.customerHandoff").cast("string").alias("customer_handoff"),
        # STRUCT: business_partner
        col("transactions.transactionInfo.businessPartner").alias("business_partner"),
        coalesce(col("transactions.transactionInfo.pos"), col("transactionSourceApplicationTypeCode"))
        .cast("string").alias("pos"),
        coalesce(col("transactions.transactionInfo.workstationId"), col("workstationId"))
        .cast("string").alias("workstation_id"),
        coalesce(
            col("transactions.transactionInfo.workstationTransactionNumber"), 
                    col("workstationTransactionNumber")
        )
        .cast("string").alias("workstation_transaction_number"),
        col("transactions.transactionInfo.refundId").cast("string").alias("refund_id"),
        coalesce(col("transactions.transactionInfo.modality"), col("modality"))
        .cast("string").alias("modality"),
        col("fulfillmentMethod").cast('string').alias("fulfillment_method"),
        # Derived values
        to_date(coalesce(col("transactions.transactionInfo.transactionDateTimeLocal"), 
                         col("transactionDateTimeLocal")))
        .alias("transaction_date_local"),
        substring(coalesce(col("transactions.transactionInfo.transactionDateTimeLocal"), 
                    col("transactionDateTimeLocal")), 12, 8)
        .alias("transaction_time_local"),
        #col("transactions.transactionInfo.terminalModality").alias("terminal_modality"),
        #col("transactions.transactionInfo.businessPartnership").alias("business_partnership")
        expr1.alias("terminal_modality"),
        expr2.alias("business_partnership")
    )
    return ftt_transaction


def silver_flatten(
    silver_df: DataFrame,
    md_batch_id: str,
    catalog_name: str,
    schema_name: str,
    tracker:Any= None, 
) -> None:
    # from diem.monitoring.tracker import MedallionLayer
    global total_record_count_input, total_record_processed_transaction, total_record_transaction_input
    # Start tracking the flattening process
    tracker.start("transactions-silver_flatten", {"md_batch_id": md_batch_id})
    try:
        silver_df = silver_df.filter(col("_change_type") == "insert")
        total_record_count_input += silver_df.count()
        # Flatten the DataFrame
        silver_df = silver_df.withColumnRenamed("com_kroger_desp_events_rss_rsssalestransaction", 
                                                "transactions")
        
        silver_df = silver_df.withColumnRenamed("transactionId", "transaction_id")
        flatten_df = flatten_transaction_level(silver_df)
        current_timestamp = datetime.now(timezone.utc)
        flatten_df = (
            flatten_df.withColumn("created_timestamp", lit(current_timestamp))
            .withColumn("modified_timestamp", lit(current_timestamp))
            # .withColumn("data_source", lit("RSS"))
        )
        (
            flatten_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "false")
            .saveAsTable(f"{catalog_name}.{schema_name}.transactions_v0")
        )
        silver_flatten_records = flatten_df.count()
        # total_record_processed_transaction += silver_flatten_records
        total_record_transaction_input += silver_flatten_records
        total_record_processed_transaction += silver_flatten_records
        # Stop tracking and record the duration
        tracker.stop(
            "transactions-silver_flatten",
            {
                "md_batch_id": md_batch_id,
                "total_record_count_input": silver_flatten_records,
                "total_record_processed_transaction": total_record_processed_transaction,
            },
        )
    except Exception as e:
        # Handle exceptions by recording the error and re-raise it
        tracker.handle(type(e).__name__, e)
        raise e


def execute_task(task_data: dict) -> None:
    # All import statements related to custom modules must be imported now, after the system path has been adjusted.
    global total_record_processed, duration, total_record_count_input, md_batch_id
    from diem.ctx import spark
    from diem.monitoring.tracker import Application, Tracker
    from functools import partial
    from diem.monitoring.tracker import MedallionLayer
    from bds_transactions.tasks.md_table_help import insert_into_job_md_table

    job_id: str = task_data.get("job-id")
    run_id: str = task_data.get("run-id")
    tracker = Tracker(
        Application(
            domain="bds",
            name="dbx_bds_trans_transaction_v0",
            job_id=job_id,
            run_id=run_id,
            transaction_id=None,
        )
    )
    # Start tracking the execution of the silver transaction flatten process
    tracker.start("transactions-execute_task", {"md_batch_id": md_batch_id})
    job_start_time = datetime.now(timezone.utc)
    schema_name = task_data["silver"]
    catalog_name = task_data["catalog"]
    table_name = "transactions_v0"
    repo = task_data["repo"]
    bronze_schema = task_data["bronze"]
    try:
        # Read streaming data from the validated silver table
        silver_df = (
            spark.readStream.format("delta")
            .option("ignoreDeletes", "true")
            .option("readChangeFeed", "true")
            .table(f"{catalog_name}.{bronze_schema}.bronze_v0")
        )
        # Define the processing and writing logic for each micro-batch
        response = (
            silver_df.writeStream.format("delta")
            # .option("checkpointLocation", f"/mnt/data/{repo}/bds/checkpoints/silver/bds_silver_transaction")
            .option(
                "checkpointLocation",
                f"/mnt/gold/{repo}/{schema_name}/bds/checkpoints/silver/{table_name}",
            )
            .option("mergeSchema", "true")
            .outputMode("update")
            .foreachBatch(
                partial(
                    silver_flatten,
                    schema_name=schema_name,
                    catalog_name=catalog_name,
                    tracker=tracker,
                )
            )
            .trigger(availableNow=True)
            .start()
        )

        if isinstance(response, dict):
            print(json.dumps(response, indent=2))

        tracker.stop(
            "transactions-executetask",
            {"response": response.status, "md_batch_id": md_batch_id},
        )

        spark.streams.awaitAnyTermination()
        job_end_time = datetime.now(timezone.utc)
        dbutils.jobs.taskValues.set(
            key=table_name, value=total_record_processed_transaction
        )
        # dbutils.jobs.taskValues.set(key = "table_names", value = table_name)
        dbutils.jobs.taskValues.set(
            key="total_count", value=total_record_processed_transaction
        )
        tracker.record(
            MedallionLayer.SILVER,
            total_record_transaction_input,
            total_record_processed_transaction,
            total_record_processed_transaction - total_record_transaction_input,
            job_end_time - job_start_time,
            {
                "md_batch_id": md_batch_id,
                "md_job_name": "DBX_BDS_TRANS_TRANSACTION_V0",
                "total_original_input": total_record_count_input,
                "schema": task_data["config-section"],
                "table_name": table_name,
            },
        )
        # Insert job metadata into the job metadata table
        insert_into_job_md_table(
            catalog_name=catalog_name,
            schema_name=bronze_schema,
            MD_BATCH_ID=md_batch_id,
            MD_JOB_NAME="DBX_BDS_TRANS_TRANSACTION_V0",
            MD_BATCH_LAYER="SILVER",
            MD_JOB_START_TIME=job_start_time,
            MD_JOB_END_TIME=job_end_time,
            MD_JOB_STATUS="SUCCESS",
            MD_JOB_RECORD_COUNT_INPUT=total_record_count_input,
            MD_JOB_RECORD_COUNT_PROCESSED=total_record_processed_transaction,
            MD_JOB_DETAILS="Transaction Level Flattening Details",
        )
        dbutils.jobs.taskValues.set(key="job_status", value="SUCCESS")
    except Exception as e:
        # Handle exceptions by recording the error and re-raise it
        tracker.handle(type(e).__name__, e)
        dbutils.jobs.taskValues.set(key="job_status", value="FAILURE")

        insert_into_job_md_table(
            catalog_name=catalog_name,
            schema_name=bronze_schema,
            MD_BATCH_ID=md_batch_id,
            MD_JOB_NAME="DBX_BDS_TRANS_TRANSACTION_V0",
            MD_BATCH_LAYER="SILVER",
            MD_JOB_START_TIME=job_start_time,
            MD_JOB_END_TIME=datetime.now(timezone.utc),
            MD_JOB_STATUS="FAILURE",
            MD_JOB_FAILURE_REASON=str(e),
            MD_JOB_RECORD_COUNT_INPUT=total_record_count_input,
            MD_JOB_RECORD_COUNT_PROCESSED=total_record_processed_transaction,
        )

        raise e

    finally:
        # Shutdown the tracker
        tracker.shutdown()


def main_cmd() -> None:  # pragma: no cover
    sys.path.append(os.path.join("..", "..", ".."))
    from diem.tasks.task_common import main
    main(None, None, execute_task)


if __name__ == "__main__":  # pragma: no cover
    main_cmd()

