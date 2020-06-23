import sys
import json
import re
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary
#spark-submit area-storenumber.py output-1 area.csv dsareastorenumber


def main(inputs,input_area,output):
    observation_schema = types.StructType([
        types.StructField('county_area', types.StringType(), True),
        types.StructField('area', types.FloatType(), True),
        ])

    etl_data = spark.read.option('multiline', True).parquet(inputs)
    area_data = spark.read.option('delimiter', '\t').csv(input_area, schema = observation_schema)
    area_data1 = area_data.withColumn('county_area',functions.lower(functions.col('county_area')))
    etl_data1 = etl_data.select('county','sale','storenumber','storename')
    etl_data2 = etl_data1.groupBy('county').agg(functions.count('storenumber').alias('no of stores in a county'))
    #etl_data1.show()
    area_etl = etl_data2.join(area_data1,area_data1.county_area == etl_data2.county)
    #area_etl.show()
    area_etl.coalesce(1).write.csv(output, mode='overwrite')
    
    #etl_data1.write.parquet(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    input_area = sys.argv[2]
    output = sys.argv[3]
    main(inputs,input_area,output)