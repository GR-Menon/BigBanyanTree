import argparse

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
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

from warc_sample_pipeline.ip_maxmind import ip_utils

reader_broadcast = spark.sparkContext.broadcast(
    ip_utils.SerializableReader("/opt/workspace/datasets/maxmind/GeoLite2-City_20240820/GeoLite2-City.mmdb"))

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


def get_ip_info(ip):
    reader = reader_broadcast.value.get_reader()
    response = reader.city(ip)
    return (
        response.traits.is_anonymous_proxy,
        response.traits.is_satellite_provider,
        response.traits.is_hosting_provider,
        response.postal.code,
        response.location.latitude,
        response.location.longitude,
        response.location.accuracy_radius,
        response.traits.is_anycast,
        response.continent.code,
        response.continent.name,
        response.country.iso_code,
        response.country.name,
        response.subdivisions[0].iso_code if response.subdivisions else None,
        response.subdivisions[0].name if response.subdivisions else None,
        response.city.name,
        response.location.metro_code,
        response.location.time_zone,
        response.represented_country.is_in_european_union
    )


get_ip_info_udf = udf(get_ip_info, ip_info_schema)

result_df = warciphost_df.withColumn("ip_info", get_ip_info_udf("ip"))
final_df = result_df.select("ip", "ip_info.*")

final_df.write.mode("append").csv(args.output_dir, header=True)
