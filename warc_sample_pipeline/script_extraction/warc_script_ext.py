import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from selectolax.parser import HTMLParser
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

################# Spark Session ######################
spark = SparkSession.builder \
    .appName("script_extraction") \
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

################# Argument Parsing ######################
parser = argparse.ArgumentParser()
parser.add_argument("--input_file", help="path to a text file that has WARC paths")
parser.add_argument("--output_dir", help="output path to save processed results")
args = parser.parse_args()

################# Broadcast Variables ######################
email_regex = re.compile(r"(mailto:)?([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", re.IGNORECASE)
broadcast_email_regex = spark.sparkContext.broadcast(email_regex)

################# DataFrame Schema ######################
output_schema = StructType([
    StructField("ip", StringType(), True),
    StructField("host", StringType(), True),
    StructField("server", StringType(), True),
    StructField("emails", ArrayType(StringType()), True),
    StructField("script_src_attrs", ArrayType(StringType()), True),
])


################# WARC Processing Functions ######################
def encode_byte_stream(input_stream):
    if input_stream is None:
        return None
    return input_stream.encode('utf-8').decode("unicode_escape").encode("latin-1").decode("utf-8", errors="replace")


def process_record(record: ArcWarcRecord):
    """Return tuple containing ip, url, server and extracted scripts and emails if record is of response type"""
    if record.rec_type == "response":
        ip = record.rec_headers.get_header("WARC-IP-Address", "-")
        url = record.rec_headers.get_header("WARC-Target-URI", "-")
        server = record.http_headers.get_header("Server")

        raw_text = record.raw_stream.read()
        str_text = str(raw_text).strip().lower().replace('\t', " ").replace('\n', "")
        slax_txt = HTMLParser(str_text)

        # Extract email addresses
        emails = [
            encode_byte_stream(atag.attributes.get("href"))
            for atag in slax_txt.tags("a")
            if atag.attributes.get("href") and broadcast_email_regex.value.match(atag.attributes.get("href"))
        ]

        # Extract script src attributes
        src_attrs = [
            script.attributes.get('src')
            for script in slax_txt.tags('script')
            if script.attributes.get('src')
        ]

        return (ip, url, server, emails, src_attrs)
    return None


def process_warc(filepath):
    """Read WARC file and yield processed records"""
    with open(filepath, 'rb') as stream:
        for record in ArchiveIterator(stream):
            result = process_record(record)
            if result:
                yield result


def proc_wrapper(_id, iterator):
    """Wrapper function for `process_warc` to handle multiple WARC files"""
    for filepath in iterator:
        for res in process_warc(filepath):
            yield res


################# Main Execution ######################
# Load the WARC file paths from the input file
data_files = spark.sparkContext.textFile(args.input_file)

# Repartition to manage the workload
data_files = data_files.repartition(10)

# Process WARC files and extract data
output = data_files.mapPartitionsWithIndex(proc_wrapper)

# Convert the processed RDD to a DataFrame
df = spark.createDataFrame(output, output_schema)

# Handle array columns by concatenating the arrays
array_columns = ['emails', 'script_src_attrs']
for col_name in array_columns:
    df = df.withColumn(col_name, concat_ws("|", col(col_name)))

# Repartition for better write performance and save as Parquet
df.repartition(1).write.mode("overwrite").parquet(args.output_dir)
