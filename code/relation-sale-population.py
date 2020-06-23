import sys
import json
import re
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import plotly
import pandas as pd
import seaborn as sns
#plotly.offline.init_notebook_mode(connected=True)
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary


#another data set popluation.csv is used 
#here i have tried to find the relation between the sales and county population and it is observed that both are directly proportional to each other
#command to run : spark-submit relation-sale-population.py  population.csv output-1 dssalepopulation


def main(input_population, input_data,output):
    observation_schema1 = types.StructType([
        types.StructField('serialnumber', types.StringType(), True),
        types.StructField('countyp', types.StringType(), True),
        types.StructField('population', types.StringType(),True ),
        ])

    population_data = spark.read.option('delimiter', '\t').csv(input_population, schema = observation_schema1)
    population_data1 = population_data.withColumn('population_county',functions.split(('countyp'),' ').getItem(0))
    population_data2 = population_data1.withColumn('population_county',functions.lower(functions.col('population_county')))
    population_data3 = population_data2.withColumn('population',population_data2.population)
    population_data4 = population_data3.select('population_county','population')
    #population_data4.show(population_data4.count(),False)
    etl_data = spark.read.option('multiline', True).parquet(input_data)
    etl_data1 = etl_data.select('county','sale','year')
    etl_data2 = etl_data1.groupBy('county').agg(functions.sum('sale').alias('totalsales'))
    sale_population1 = etl_data2.join(population_data4, population_data4.population_county == etl_data2.county)
    sale_population2 = sale_population1.select('county','totalsales','population')
    sale_population3 = sale_population2.filter(~reduce(lambda x, y: x | y, [sale_population2[c].isNull() for c in sale_population2.columns]))
    #sale_population4 = sale_population3.sort(functions.desc('population'))
    #sale_population3.show(sale_population1.count(),False)
    sale_population3.coalesce(1).write.csv(output, mode='overwrite')
   


if __name__ == '__main__':
    input_population = sys.argv[1]
    input_data = sys.argv[2]
    output = sys.argv[3]
    main(input_population,input_data,output)