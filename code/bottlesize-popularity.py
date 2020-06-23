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
#there are 129 distinct categories of alocohol 
#spark-submit bottlesize-popularity.py output-1 dspopularbottlesize

def main(input,output):
    etl_data = spark.read.option('multiline', True).parquet(input)
    data1 = etl_data.groupBy('bottlevolume').agg(functions.sum('bottlessold').alias('bottlessold'),functions.sum('sale').alias('totalsales'))
    #data1.show(data1.count(),False)
    data2 = data1.filter(~reduce(lambda x: x, [data1['bottlevolume'] == 0]))
    #data2.show(data2.count(),False)
    data2.coalesce(1).write.csv(output, mode='overwrite')



if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input,output)