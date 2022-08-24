import argparse
import ipaddress
from pyspark.sql.functions import concat_ws, col, sha2, udf
from dependencies import schemas
from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--current_file_path",  help="Path of most recent version of the dataset CSV file", required=True)
    parser.add_argument("--previous_file_path", help="Path of previous version of the dataset CSV file", required=True)
    parser.add_argument("--output_dir",         help="Output directory of application", required=True)
    args = parser.parse_args()

    # start Spark application and get Spark session, logger and config
    spark, log, config = start_spark(
        app_name='my_etl_job',
        files=['configs/etl_config.json'])

    # UDF Define
    first_ip = udf(lambda range: str(int(ipaddress.ip_network(range)[0])))
    last_ip =  udf(lambda range: str(int(ipaddress.ip_network(range)[-1])))

    # log that main ETL job is starting
    log.info('etl_job is up-and-running')

    # execute ETL pipeline
    #CURRENT
    current_df = extract_data(spark, args.current_file_path)
    current_df = current_df.withColumn('hash_key', sha2(concat_ws('-', *current_df.columns),256))\
        .withColumn("first_ip", first_ip(col('range')))\
        .withColumn("last_ip", last_ip(col('range'))) #We add row hash_key

    log.info("Done adding hash for may")
    current_df.write.format("csv").mode("overwrite").option("delimiter","|").option("escape", "").option("quote", "").save(args.output_dir+"/current")

    #PREVIOUS
    previous_df = extract_data(spark, args.previous_file_path)
    previous_df = previous_df.withColumn('hash_key', sha2(concat_ws('-', *previous_df.columns),256))\
        .withColumn("first_ip", first_ip(col('range')))\
        .withColumn("last_ip", last_ip(col('range')))
    log.info("Done adding hash for april")
    previous_df.write.format("csv").mode("overwrite").option("delimiter","|").option("escape", "").option("quote", "").save(args.output_dir+"/previous")

    previous_df.createOrReplaceTempView("previous")
    current_df.createOrReplaceTempView("current")

    #CHANGED RECORDS
    changed_current_df = spark.sql("""
        select current.*, previous.hash_key as phash_key
        from current
        left outer join previous on  previous.range = current.range
        where previous.hash_key is distinct from current.hash_key
    """)
    changed_current_df.write.format("csv").mode("overwrite").option("delimiter","|").option("escape", "").option("quote", "").save(args.output_dir+"/changed_current")

    changed_previous_df = spark.sql("""
        select previous.*, current.hash_key as chash_key
        from previous
        left outer join current on  previous.range = current.range 
        where previous.hash_key is distinct from current.hash_key 
    """)
    changed_previous_df.write.format("csv").mode("overwrite").option("delimiter","|").option("escape", "").option("quote", "").save(args.output_dir+"/changed_previous")


    spark.stop()
    return None


def extract_data(spark, path):
    """Load data from CSV file format.
    """
    df = spark.read.format("csv").schema(schemas.input_schema()).load(path)
    return df

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
