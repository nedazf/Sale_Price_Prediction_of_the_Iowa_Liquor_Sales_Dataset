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

#calculating the profit vendor wise for state
# command to run : spark-submit vendor-profit.py output-1 dsvendorprofit


def main(inputs,output):

    etl_data = spark.read.option('multiline', True).parquet(inputs)
    data2 = etl_data.select('vendorname','statebottlecost','statebottleretail','pack','bottlessold','vendornumber')
    data3 = data2.withColumn('vendorsp',data2.statebottleretail * data2.bottlessold) #here each vendor selling price is calculated
    data3.show()
    data4 = data3.withColumn('vendorcp',data2.statebottleretail * data2.pack) #here cost of each bootle for which vendor paid is calculated
    data4.show()
    data5 = data4.select('vendornumber','vendorname','vendorsp','vendorcp')
    data6 = data5.groupBy('vendornumber','vendorname').agg(functions.sum('vendorcp').alias('vendorcp'),functions.sum('vendorsp').alias('vendorsp'))
    data6.show()
    data9 = data6.withColumn('profit/loss',data6.vendorsp - data6.vendorcp)
    data9 = data9.sort(functions.desc('profit/loss'))
    data7 = data9.select('vendornumber','vendorname','profit/loss')
    #data7.show()
    data7.coalesce(1).write.csv(output, mode='overwrite')


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)