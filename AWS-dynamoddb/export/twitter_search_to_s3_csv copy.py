'''
Export data from the dynamodb table Twitter to a csv file in a s3 bucket
Transformations:
- remove newlines and tabs

For AWS glue pyspark documentation (ETL programming in Python), e.g. on dynamicframe methods etc. see
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html
'''

import sys
import pyspark.sql.functions as f
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#twitter
datasource_tw = glueContext.create_dynamic_frame.from_catalog(database = "dynamodb", table_name = "twitter")
dynframe_twitter = ApplyMapping.apply(frame = datasource_tw, mappings = [("symbol", "string", "symbol", "string"), ("full_text", "string", "full_text", "string"), ("created_at", "string", "created_at", "string"), ("id", "long", "id", "long"), ("url", "string", "url", "string")])

#convert aws glue dynamicframes to spark dataframes
tw = dynframe_twitter.toDF()

#remove [\\n\\t\$#]
tw = tw.withColumn("full_text", f.regexp_replace(f.col("full_text"), "[\\n\\t\$#]", ""))

#convert spark dataframes back to aws glue dynamicframes
dynframe_twitter = DynamicFrame.fromDF(tw, glueContext, "nested")
  
#partition to 1 to get a single s3 file as output
dynframe_output = dynframe_twitter.repartition(1)

datasink = glueContext.write_dynamic_frame.from_options(frame = dynframe_output, connection_type = "s3", connection_options = {"path": "s3://541304926041-twitter"}, format = "csv")
job.commit()
