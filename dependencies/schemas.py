from pyspark.sql.types import StructType, StructField, StringType

def input_schema():
    return StructType([
        StructField("range", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", StringType(), True),
        StructField("lng", StringType(), True),
        StructField("postal", StringType(), True),
        StructField("region", StringType(), True),
        StructField("source", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("geoname_id", StringType(), True)
        ])