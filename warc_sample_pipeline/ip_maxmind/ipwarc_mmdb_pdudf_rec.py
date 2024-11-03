import argparse

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType
)
from warcio import ArchiveIterator

spark = SparkSession.builder \
    .appName("maxmind-warc") \
    .master("spark://spark-master:7077") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:///opt/spark/spark-events/") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", 1) \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.dynamicAllocation.initialExecutors", 2) \
    .config("spark.dynamicAllocation.minExecutors", 2) \
    .config("spark.dynamicAllocation.maxExecutors", 15) \
    .config("spark.scheduler.mode", "FAIR") \
    .getOrCreate()

# add `ip_utils.py` for workers to be able to use
# the custom `SerializableReader` class.
spark.sparkContext.addPyFile("./ip_utils.py")

parser = argparse.ArgumentParser()
parser.add_argument("--input_file", help="path to a text file that has warc paths")
parser.add_argument("--output_dir", help="output path to save processed results")
args = parser.parse_args()


def process_record(record):
    """Return tuple containing ip, url if record is of response type"""
    if record.rec_type == "response":
        ip = record.rec_headers.get_header("WARC-IP-Address", "-")
        url = record.rec_headers.get_header("WARC-Target-URI", "-")
        server = record.http_headers.get_header("Server")
        return (ip, url, server)
    return None


def process_warc(filepath):
    """Read WARC file and yield processed records"""
    with open(filepath, 'rb') as stream:
        for record in ArchiveIterator(stream):
            result = process_record(record)
            if result:
                yield result


def proc_wrapper(_id, iterator):
    """Wrapper function for `process_warc` to handle multiple `warc` files"""
    for filepath in iterator:
        for res in process_warc(filepath):
            yield res


output_schema = StructType([
    StructField("ip", StringType(), True),
    StructField("host", StringType(), True),
    StructField("server", StringType(), True)
])

data_files = spark.sparkContext.textFile(args.input_file)
data_files = data_files.repartition(3)
output = data_files.mapPartitionsWithIndex(proc_wrapper)

warciphost_df = spark.createDataFrame(output, schema=output_schema)

# broadcast the `SerializableReader` object from which the reader is created lazily inside the UDF.
# have a look at `get_ip_info`.
from warc_sample_pipeline.ip_maxmind import ip_utils

reader_broadcast = spark.sparkContext.broadcast(
    ip_utils.SerializableReader("/opt/workspace/datasets/maxmind/GeoLite2-City_20240820/GeoLite2-City.mmdb",
                                "/opt/workspace/warc_sample_pipeline/mmdb_errors.txt"))

ip_info_schema = StructType([
    StructField("postal_code", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("accuracy_radius", IntegerType(), True),
    StructField("continent_code", StringType(), True),
    StructField("continent_name", StringType(), True),
    StructField("country_iso_code", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("subdivision_iso_code", StringType(), True),
    StructField("subdivision_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("metro_code", IntegerType(), True),
    StructField("time_zone", StringType(), True),
])


@pandas_udf(ip_info_schema)
def get_ip_info(ips: pd.Series) -> pd.DataFrame:
    """
    Get reader, query MMDB for info about each IP from a batch of IPs, and return a DataFrame.
    The reader object is created lazily, once per executor.
    Have a look at `SerializableReader` in the `ip_utils` module for more details.
    """
    result = []
    reader = reader_broadcast.value.get_reader()

    for ip in ips:
        try:
            response = reader.city(ip)
            result.append((
                response.postal.code if response else None,
                response.location.latitude if response else None,
                response.location.longitude if response else None,
                response.location.accuracy_radius if response else None,
                response.continent.code if response else None,
                response.continent.name if response else None,
                response.country.iso_code if response else None,
                response.country.name if response else None,
                response.subdivisions[0].iso_code if response and response.subdivisions else None,
                response.subdivisions[0].name if response and response.subdivisions else None,
                response.city.name if response else None,
                response.location.metro_code if response else None,
                response.location.time_zone if response else None,
            ))
        except Exception as e:
            result.append((None,) * len(ip_info_schema))

    return pd.DataFrame(result, columns=ip_info_schema.names)


result_df = warciphost_df.withColumn("ip_info", get_ip_info("ip"))

# separate the newly created `ip_info.*` columns
final_df = result_df.select("ip", "host", "server", "ip_info.*").dropna("all")
final_df = final_df.dropDuplicates(["ip"]).repartition(1)

final_df.write.mode("append").csv(args.output_dir, header=True)

reader_broadcast.value.close()
reader_broadcast.unpersist()
