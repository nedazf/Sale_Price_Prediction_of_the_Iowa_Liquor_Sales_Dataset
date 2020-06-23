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

#calculating the average expenditure by each person on alocohol purchase
#here total sales grouped by county, year is divided by population to get average expenditure
#another data set called as income.csv is used
#another data set popluation.csv is used to get the per head alocohol expenditure
#idea - per head alcohol consumption county wise
#13 % of population under 18
#command to run : spark-submit expenditure-county.py income.csv population.csv output-1 dsexpenditure

#analysis of state can be done totalsale and population exp per head

def main(input_income, input_population, input_data,output):
    observation_schema = types.StructType([
        types.StructField('year', types.StringType(), True),
        types.StructField('countyname', types.StringType(), True),
        types.StructField('geolocation', types.StringType(),True ),
        types.StructField('income', types.FloatType(), True),
        ])

    observation_schema1 = types.StructType([
        types.StructField('serialnumber', types.StringType(), True),
        types.StructField('countyp', types.StringType(), True),
        types.StructField('population', types.StringType(),True ),
        ])

    income_data = spark.read.csv(input_income, schema = observation_schema)
    income_data1 = income_data.filter(income_data.countyname.isNotNull())
    income_data2 = income_data1.withColumn('income_county',functions.split(('countyname'),' ').getItem(0))
    income_data3 = income_data2.withColumn('income_county',functions.lower(functions.col('income_county')))
    income_data4 = income_data3.select('income_county','year','income').sort('income_county')
    income_data4 = income_data4.withColumnRenamed('year','income_year')
    #income_data4.show(income_data4.count(),False)
    population_data = spark.read.option('delimiter', '\t').csv(input_population, schema = observation_schema1)
    population_data1 = population_data.withColumn('population_county',functions.split(('countyp'),' ').getItem(0))
    population_data2 = population_data1.withColumn('population_county',functions.lower(functions.col('population_county')))
    population_data3 = population_data2.withColumn('population18',population_data2.population*0.13)
    population_data4 = population_data3.select('population_county','population18')
    #population_data4.show(population_data4.count(),False)
    etl_data = spark.read.option('multiline', True).parquet(input_data)
    etl_data1 = etl_data.select('county','sale','year')
    etl_data2 = etl_data1.groupBy('county','year').agg(functions.sum('sale').alias('totalsales'))
    sale_population = etl_data2.join(population_data4, population_data4.population_county == etl_data2.county)
    sale_population1 = sale_population.withColumn('expenditure_person',sale_population.totalsales / sale_population.population18)
    sale_population2 = sale_population1.join(income_data4, (sale_population1.county == income_data4.income_county) & (sale_population1.year == income_data4.income_year))
    sale_population3 = sale_population2.select('county','year','totalsales','expenditure_person','income').sort('county')
    sale_population4 = sale_population3.withColumn('expenditure_person',sale_population3.expenditure_person*10)
    sale_population5 = sale_population4.filter(~reduce(lambda x, y: x | y, [sale_population3[c].isNull() for c in sale_population3.columns]))
    #sale_population5.show(sale_population1.count(),False)
    sale_population5.coalesce(1).write.csv(output, mode='overwrite')
    #income_data3.show()
    #etl_data2.show()
    #sale_population1.show()


if __name__ == '__main__':
    input_income = sys.argv[1]
    input_population = sys.argv[2]
    input_data = sys.argv[3]
    output = sys.argv[4]
    main(input_income,input_population,input_data,output)