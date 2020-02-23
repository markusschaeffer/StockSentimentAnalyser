'''
Export data from the dynamodb table Stocktwits to a csv file in a s3 bucket
Transformations:
- Bullish and Bearish records only
- lower()
- remove newlines and tabs
- remove symbols/tickers ($ticker and #ticker)
- remove stock names

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

#stocktwits
datasource_stw = glueContext.create_dynamic_frame.from_catalog(database = "dynamodb", table_name = "stocktwits")
dynframe_stocktwits = ApplyMapping.apply(frame = datasource_stw, mappings = [("createdat", "string", "createdat", "string"), ("symbol", "string", "symbol", "string"), ("id", "string", "id", "string"), ("body", "string", "body", "string"), ("stocktwitssentiment", "string", "stocktwitssentiment", "string")])

#watchlist
datasource_wl = glueContext.create_dynamic_frame.from_catalog(database = "dynamodb", table_name = "watchlist")
dynframe_watchlist = ApplyMapping.apply(frame = datasource_wl, mappings = [("symbol", "string", "symbol", "string"), ("name", "string", "name", "string")])

#convert aws glue dynamicframes to spark dataframes
wl = dynframe_watchlist.toDF()
stw = dynframe_stocktwits.toDF()

#lower text in columns
wl = wl.withColumn("name", f.lower(wl.name))
wl = wl.withColumn("symbol", f.lower(wl.symbol))
stw = stw.withColumn("body", f.lower(stw.body))

#filter on bullish and bearish sentiment only, remove stocktwitssentiment=none
stw = stw.filter((stw.stocktwitssentiment=="Bullish") | (stw.stocktwitssentiment=="Bearish"))

#transform time format
#from e.g. 2019-10-25T00:11:11Z to 2019-10-25 00:11:11
stw = stw.withColumn("createdat", f.regexp_replace(f.col("createdat"), "[T]", " "))
stw = stw.withColumn("createdat", f.regexp_replace(f.col("createdat"), "[Z]", ""))

#remove [\\n\\t\$#]
stw = stw.withColumn("body", f.regexp_replace(f.col("body"), "[\\n\\t\$#]", ""))

#remove symbols from text
for row in wl.select('symbol').collect():
  stw = stw.withColumn("body", f.regexp_replace(f.col("body"), row[0], ""))

#remove stock name
for row in wl.select('name').collect():
  stw = stw.withColumn("body", f.regexp_replace(f.col("body"), row[0], ""))

#convert spark dataframes back to aws glue dynamicframes
dynframe_stocktwits = DynamicFrame.fromDF(stw, glueContext, "nested")
  
#partition to 1 to get a single s3 file as output
dynframe_output = dynframe_stocktwits.repartition(1)

datasink = glueContext.write_dynamic_frame.from_options(frame = dynframe_output, connection_type = "s3", connection_options = {"path": "s3://541304926041-stocktwits"}, format = "csv")
job.commit()
