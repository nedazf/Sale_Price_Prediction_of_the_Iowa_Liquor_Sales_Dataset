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

#calculating the profit county wise for state
#spark-submit county-profit.py output-1 dscountyprofit


def main(inputs,output):

    etl_data = spark.read.option('multiline', True).parquet(inputs)
    data1 = etl_data.select('county','statebottleretail','statebottlecost','pack')
    data2 = data1.groupBy('county').agg(functions.sum('statebottleretail').alias('totalretail')).sort('county')
    #data2.show()
    data3 = data1.groupBy('county').agg(functions.sum('statebottlecost').alias('totalcost')).sort('county')
    data3 = data3.withColumnRenamed('county','county1')
    #data3.show()
    data4 = data2.join(data3,data2.county == data3.county1)
    data4 = data4.select('county','totalretail','totalcost')
    #data4.show()
    data5 = data4.withColumn('profit',data4.totalretail - data4.totalcost)
    data5 = data5.select('county','totalretail','totalcost',functions.round('profit',4).alias('profit'))
    data5 = data5.sort(functions.desc('profit'))
    #data5.show()
    data5.coalesce(1).write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)