import argparse

import pandas as pd
from pyspark.sql import Row
from pyspark.sql.functions import pandas_udf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType
)
from warcio import ArchiveIterator

spark = SparkSession.builder \
    .appName("maxmind-warc") \
    .getOrCreate()

# add `ip_utils.py` for workers to be able to use
# the custom `SerializableReader` class.
spark.sparkContext.addPyFile("./ip_utils.py")

parser = argparse.ArgumentParser()
parser.add_argument("--warc_file", help="path to warc file to process")
parser.add_argument("--output_dir", help="output path to save processed results")
args = parser.parse_args()

def process_record(record):
    """Return tuple containing ip, url if record is of response type"""
    if record.rec_type == "response":
        ip = record.rec_headers.get_header("WARC-IP-Address", "-")
        url = record.rec_headers.get_header("WARC-Target-URI", "-")
        return (ip, url)
    return None
        
def process_warc(filepath):
    """Read WARC file and yield processed records"""
    with open(filepath, 'rb') as stream:
        for record in ArchiveIterator(stream):
            result = process_record(record)
            if result:
                yield result

data = []
for record in process_warc(args.warc_file):
    res = {"ip": record[0], "hostname": record[1]}
    data.append(res)

warciphost_df = spark.createDataFrame([Row(**d) for d in data])

# broadcasts the `SerializableReader` object from which the reader is created lazily inside the UDF.
# also look at `get_ip_info`.
import ip_utils
reader_broadcast = spark.sparkContext.broadcast(ip_utils.SerializableReader("/opt/workspace/datasets/maxmind/GeoLite2-City_20240820/GeoLite2-City.mmdb", "mmdb_errors.txt"))

ip_info_schema = StructType([
    StructField("is_anonymous_proxy", BooleanType(), True),
    StructField("is_satellite_provider", BooleanType(), True),
    StructField("is_hosting_provider", BooleanType(), True),
    StructField("postal_code", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("accuracy_radius", IntegerType(), True),
    StructField("is_anycast", BooleanType(), True),
    StructField("continent_code", StringType(), True),
    StructField("continent_name", StringType(), True),
    StructField("country_iso_code", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("subdivision_iso_code", StringType(), True),
    StructField("subdivision_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("metro_code", IntegerType(), True),
    StructField("time_zone", StringType(), True),
    StructField("is_in_european_union", BooleanType(), True)
])

@pandas_udf(ip_info_schema)
def get_ip_info(ips: pd.Series) -> pd.DataFrame:
    """
    Get reader, query MMDB for info about each IP from a batch of IPs, and return a DataFrame.
    The reader object is created lazily, once per executor.
    Have a look at `SerializableReader` in the `ip_utils` module for more details.

    Performance details:
        - normal Python UDF takes about 9 seconds to process 1 WARC file (tested on only 1).
        - Pandas UDF takes about 9 seconds too to process 1 WARC file (tested on only 1).

    TODO:
        - run on a batch of files and compare performance.
    """
    result = []
    reader = reader_broadcast.value.get_reader()
    for ip in ips:
        try:
            response = reader.city(ip)
        except Exception as e:
            errfile = reader_broadcast.value.get_errfile()
            print(e, file=errfile)
            
        result.append({
            "is_anonymous_proxy": response.traits.is_anonymous_proxy,
            "is_satellite_provider": response.traits.is_satellite_provider,
            "is_hosting_provider": response.traits.is_hosting_provider,
            "postal_code": response.postal.code,
            "latitude": response.location.latitude,
            "longitude": response.location.longitude,
            "accuracy_radius": response.location.accuracy_radius,
            "is_anycast": response.traits.is_anycast,
            "continent_code": response.continent.code,
            "continent_name": response.continent.name,
            "country_iso_code": response.country.iso_code,
            "country_name": response.country.name,
            "subdivision_iso_code": response.subdivisions[0].iso_code if response.subdivisions else None,
            "subdivision_name": response.subdivisions[0].name if response.subdivisions else None,
            "city_name": response.city.name,
            "metro_code": response.location.metro_code,
            "time_zone": response.location.time_zone,
            "is_in_european_union": response.represented_country.is_in_european_union
        })
    return pd.DataFrame(result)        

result_df = warciphost_df.withColumn("ip_info", get_ip_info("ip"))

# separate the newly created `ip_info.*` columns
final_df = result_df.select("ip", "ip_info.*").coalesce(1)

final_df.write.mode("append").csv(args.output_dir, header=True)