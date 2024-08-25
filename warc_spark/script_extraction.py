import os
import re
from typing import Dict, Optional 
import argparse
from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArcWarcRecord

from selectolax.parser import HTMLParser

from pyspark.sql.types import StructField, StructType, StringType, ArrayType
from pyspark.sql.functions import col, sum, when, size, round, concat_ws
from pyspark.sql import SparkSession, Row

################# Spark Session ######################
spark = SparkSession.builder \
    .appName("script_extraction") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", 1) \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.shuffleTracking.enabled", "true") \
    .config("spark.dynamicAllocation.initialExecutors", 5) \
    .config("spark.dynamicAllocation.minExecutors", 0) \
    .config("spark.dynamicAllocation.maxExecutors", 10) \
    .getOrCreate()

################# Argument Parsing ######################
parser = argparse.ArgumentParser()
parser.add_argument("--warc_file", help="path to input WARC files")
parser.add_argument("--output_dir", help="output path for processed TSV files")
args = parser.parse_args()


################# Regex & DataFrame Schema ######################
email_regex = re.compile(r"(mailto:)?([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", re.IGNORECASE)

schema = StructType([
    StructField("warc_target_uri", StringType(), True),
    StructField("warc_ip_address", StringType(), True),
    StructField("content_type", StringType(), True),
    StructField("content_length", StringType(), True),
    StructField("server", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("page_description", StringType(), True),
    StructField("emails", ArrayType(StringType()), True),
    StructField("keywords", StringType(), True),
    StructField("script_src_attrs", ArrayType(StringType()), True),
    StructField("script_type_attrs", ArrayType(StringType()), True)
])
       
################# WARC Script Extraction ######################
def encode_byte_stream(input_stream):
    if input_stream is None:
        return None
    return input_stream.encode('utf-8').decode("unicode_escape").encode("latin-1").decode("utf-8", errors="replace")
    
def record_script_extraction(record : ArcWarcRecord) -> Optional[Dict]:
    """
        Returns a dictionary containing values of various extracted HTML source tags.
        Args:
            - record (ArcWarcRecord) - Individual record stored in WARC file
    """
    if record.rec_type == "response":      
        raw_text = record.raw_stream.read()
        str_text = str(raw_text).strip().lower().replace('\t', " ").replace('\n', "")
        slax_txt = HTMLParser(str_text)

        target_uri = record.rec_headers.get_header('WARC-Target-URI')
        ip_address = record.rec_headers.get_header("WARC-IP-Address")
        content_type = record.rec_headers.get_header("Content-Type")
        content_length = record.rec_headers.get_header("Content-Length")
        server = record.http_headers.get_header("Server")
                
        title = slax_txt.tags('title')[0].text().strip() if slax_txt.tags('title') else None
        title = encode_byte_stream(title)

        # Extract emails from anchor tags
        emails = [
            encode_byte_stream(atag.attributes.get("href"))
            for atag in slax_txt.tags("a")
            if atag.attributes.get("href") and email_regex.match(atag.attributes.get("href"))
        ]

        # Extract meta tag descriptions and keywords
        desc, keywords = None, None
        meta_tags = slax_txt.tags('meta')
        for m_idx, mtag in enumerate(meta_tags):
            desc_tag = mtag.css_first('meta[name="description"]')
            key_tag = mtag.css_first('meta[name="keywords"]')
            if desc_tag:
                desc = desc_tag.attributes.get("content")
                desc = encode_byte_stream(desc)
            if key_tag:
                keywords = key_tag.attributes.get("content")
                keywords = encode_byte_stream(keywords)
                
        # Extract script src and type attributes
        src_attrs = [
            script.attributes.get('src')
            for script in slax_txt.tags('script')
            if script.attributes.get('src')
        ]

        type_attrs = [
            script.attributes.get('type')
            for script in slax_txt.tags('script')
            if script.attributes.get('type')
        ]

        record_data = {
            "warc_target_uri": target_uri,
            "warc_ip_address": ip_address,
            "content_type": content_type,
            "content_length": content_length,
            "server": server,
            "page_title": title,
            "page_description": desc,
            "emails": emails,
            "keywords": keywords,
            "script_src_attrs": src_attrs,
            "script_type_attrs": type_attrs
        }
        # if all(value is None or (isinstance(value, list) and not value) for value in record_data.values()):
        #     return None
        return record_data
    return None

def process_warc(filepath:str) -> Dict:
    """
        Returns the output for each record in WARC file.
        Args:
            - filepath (str) - Path to input WARC file
    """
    with open(filepath, 'rb') as stream:
        for record in ArchiveIterator(stream):
            result = record_script_extraction(record)
            if result:
                yield result

data = []
for record in process_warc(args.warc_file):
    if record is not None:
        data.append(record)

df = spark.createDataFrame(data, schema)
array_columns = ['emails', 'script_src_attrs', 'script_type_attrs']
for col_name in array_columns:
    df = df.withColumn(col_name, concat_ws("|", col(col_name)))

# filter_condition = " AND ".join([f"{col} IS NOT NULL" for col in df.columns])
# df = df.filter(filter_condition)

df.coalesce(1).write.option("delimiter", "\t").mode("overwrite").csv(args.output_dir, header=True)

    