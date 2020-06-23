import sys
import json
import re
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import plotly
import pandas as pd
import seaborn as sns
#plotly.offline.init_notebook_mode(connected=True)
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary

#calculating the average consumption of alcohol by each person 
#here total sales grouped by county, year is divided by population to get average volume of liquor consumed in litres
#another data set popluation.csv is used to get the per head alocohol consumption
#idea - per head alcohol consumption county wise
#13 % of population under 18
#command to run : spark-submit liquor-consumption.py population.csv output-1 dsconsumption
#to do : can relate to health expenditure and try to find the relation between consumption and health problems
# to do : try to relate it with the county with max sales 

#analysis of state can be done totalsale and population exp per head

def main(input_population, input_data,output):
    observation_schema = types.StructType([
        types.StructField('serialnumber', types.StringType(), True),
        types.StructField('countyp', types.StringType(), True),
        types.StructField('population', types.StringType(),True ),
        ])

    population_data = spark.read.option('delimiter', '\t').csv(input_population, schema = observation_schema)
    population_data1 = population_data.withColumn('population_county',functions.split(('countyp'),' ').getItem(0))
    population_data2 = population_data1.withColumn('population_county',functions.lower(functions.col('population_county')))
    population_data3 = population_data2.withColumn('population18',population_data2.population*0.13)
    population_data4 = population_data3.select('population_county','population18')
    population_data4.show(population_data4.count(),False)
    etl_data = spark.read.option('multiline', True).parquet(input_data)
    etl_data1 = etl_data.select('county','sale','year','volumesoldl')
    etl_data2 = etl_data1.groupBy('county').agg(functions.sum('volumesoldl').alias('volumesoldl'),functions.sum('sale').alias('totalsale'))
    consumption_population = etl_data2.join(population_data4, population_data4.population_county == etl_data2.county)
    consumption_population1 = consumption_population.withColumn('consumptionperperson',consumption_population.volumesoldl / consumption_population.population18)
    consumption_population3 = consumption_population1.filter(consumption_population1.consumptionperperson.isNotNull())
    consumption_population2 = consumption_population3.select('county','consumptionperperson','totalsale')
    #consumption_population2.show()
    consumption_population2.coalesce(1).write.csv(output, mode='overwrite')


if __name__ == '__main__':
    input_population = sys.argv[1]
    input_data = sys.argv[2]
    output = sys.argv[3]
    main(input_population,input_data,output)