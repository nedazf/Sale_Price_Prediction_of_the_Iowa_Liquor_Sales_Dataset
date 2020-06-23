import sys
import json
import re
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary
#spark-submit population-storenumber.py output-1 population.csv dspopulationstorenumber


def main(inputs,input_population,output):
    observation_schema1 = types.StructType([
        types.StructField('serialnumber', types.StringType(), True),
        types.StructField('countyp', types.StringType(), True),
        types.StructField('population', types.StringType(),True ),
        ])

    etl_data = spark.read.option('multiline', True).parquet(inputs)
    population_data = spark.read.option('delimiter', '\t').csv(input_population, schema = observation_schema1)
    population_data1 = population_data.withColumn('population_county',functions.split(('countyp'),' ').getItem(0))
    population_data2 = population_data1.withColumn('population_county',functions.lower(functions.col('population_county')))
    population_data3 = population_data2.select('population_county','population')
    etl_data1 = etl_data.select('county','sale','storenumber','storename')
    etl_data2 = etl_data1.groupBy('county').agg(functions.count('storenumber').alias('no of stores in a county'))
    #etl_data1.show()
    population_etl = etl_data2.join(population_data3,population_data3.population_county == etl_data2.county)
    #population_etl.show()
    population_etl.coalesce(1).write.csv(output, mode='overwrite')
    
    #etl_data1.write.parquet(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    input_population = sys.argv[2]
    output = sys.argv[3]
    main(inputs,input_population,output)