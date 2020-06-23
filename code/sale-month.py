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
#spark-submit sale-month.py output-1 dssalemonth


def main(inputs,output):
    etl_data = spark.read.option('multiline', True).parquet(inputs)
    etl_data.createOrReplaceTempView('etl_data')
    data1 = etl_data.groupBy('month','year').agg(functions.sum('sale').alias('totalsales'))
    #data1.show()
    data1.coalesce(1).write.csv(output, mode='overwrite')
    #data2 = etl_data.groupBy('categoryname').agg(functions.sum('sale').alias('sales by category')).sort('categoryname')
    #data2.show(data2.count(),False)
    #data3 = etl_data.groupBy('county').agg(functions.sum('sale').alias('sale by county')).sort('county')
    #data3.show(data3.count(),False)
    #data4 = etl_data.groupBy('month','categoryname').agg(functions.sum('sale').alias('total-sales')).sort('month')
    #data4.show(data4.count(),False)
    #data5 = etl_data.groupBy('day').agg(functions.sum('sale').alias('total-sales')).sort('day')
    #data5 = etl_data.select('day').distinct().sort('day')
    #data5.show(data5.count(),False)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)