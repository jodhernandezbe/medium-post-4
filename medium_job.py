'''
Description:
This is a customized AWS Glue Job to query weather data. This is
part of the Medium post example

Developed by: Jose D. Hernandez-Betancur

Date: March 5, 2023
'''

# Importing libraries
import os
import sys
import datetime
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import (StructType, StructField, StringType,
                              IntegerType, FloatType)
from awsglue import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from helpers import WeatherAPI, collapse_columns


# Add additional spark configurations
conf = SparkConf()
conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
conf.set("spark.sql.broadcastTimeout",  "3600")
conf.set("spark.sql.autoBroadcastJoinThreshold",  "1073741824")

# Initial configuration
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate(conf=conf))
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Reading environment variables
aws_account_id = os.environ['AWS_ACCOUND_ID']
aws_region = os.environ['AWS_REGION']


# Calling the cities name
s3_input_path = f's3://medium-post-{aws_account_id}-{aws_region}/worldcities.csv'
ddf = glueContext.create_dynamic_frame.from_options(
                      connection_type='s3',
                      connection_options={'paths': [s3_input_path]},
                      format='csv',
                      format_options={'withHeader': True},
                      transformation_ctx='datasource0')


# Initializing WeatherAPI instance
weather_instance = WeatherAPI()


# Schema
schema = StructType([
    StructField('temp_c', FloatType(), True),
    StructField('is_day', IntegerType(), True),
    StructField('wind_mph', FloatType(), True),
    StructField('wind_kph', FloatType(), True),
    StructField('wind_degree', IntegerType(), True),
    StructField('wind_dir', StringType(), True),
    StructField('pressure_mb', FloatType(), True),
    StructField('pressure_in', FloatType(), True),
    StructField('precip_mm', FloatType(), True),
    StructField('precip_in', FloatType(), True),
    StructField('humidity', IntegerType(), True),
    StructField('cloud', IntegerType(), True),
    StructField('feelslike_c', FloatType(), True),
    StructField('feelslike_f', FloatType(), True),
    StructField('vis_km', FloatType(), True),
    StructField('vis_miles', FloatType(), True),
    StructField('uv', FloatType(), True),
    StructField('gust_mph', FloatType(), True),
    StructField('gust_kph', FloatType(), True),
                    ])


# Defining a UDF in order to call the Weather API
udf_execute_api = udf(weather_instance.get_weather_data, schema).asNondeterministic()
df = ddf.toDF()
df = df.withColumn('result', udf_execute_api(col('lat'), col('lng')))


# Selecting the columns based on the desired schema
df = df.select(collapse_columns(df.schema))
print(df.take(1))
print(df.printSchema())


# Saving
ingested_at = datetime.date.today().strftime("%Y-%m-%d")
s3_output_path = f's3://medium-post-{aws_account_id}-{aws_region}/weather-{ingested_at}'
datasink1 = glueContext.write_dynamic_frame.\
                    from_options(frame=DynamicFrame.fromDF(df,
                                                        glueContext,
                                                        'datasink1'),
                                connection_type='s3',
                                format='parquet',
                                connection_options={'path': s3_output_path}
                                )
