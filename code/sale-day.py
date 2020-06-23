import sys
import json
import re
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import plotly
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession, functions, types
import datetime
import calendar
#plotly.offline.init_notebook_mode(connected=True)
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# spark-submit sale-day.py output-1 dssaleday

# add more functions as necessary
@functions.udf(returnType=types.StringType())
def week_day(date1):
    dt = date1
    month,day,year = (int(x) for x in dt.split('/'))
    day_week = datetime.date(year,month,day).weekday()
    return calendar.day_name[day_week]


def main(inputs,output):
    etl_data = spark.read.option('multiline', True).parquet(inputs)
    etl_data1 = etl_data.withColumn('dayofweek',week_day(etl_data.date))
    data1 = etl_data1.groupBy('dayofweek').agg(functions.sum('sale').alias('total-sales'))
    #etl_data1.show()
    #data1.show()
    data1.coalesce(1).write.csv(output, mode='overwrite')
    #data5 = etl_data.groupBy('day').agg(functions.sum('sale').alias('total-sales')).sort('day')
    #data5 = etl_data.select('day').distinct().sort('day')
    #data5.show(data5.count(),False)

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)