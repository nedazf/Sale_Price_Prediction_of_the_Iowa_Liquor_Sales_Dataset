import sys
import json
import re
import matplotlib.pyplot as plt
import plotly.graph_objs as go
import plotly
import plotly.plotly as py
import plotly.graph_objs as go
from plotly import tools
import pandas as pd
import numpy as np
import seaborn as sns
from bokeh.io import show, output_file
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource
from bokeh.palettes import Spectral6
plotly.tools.set_credentials_file(username='jaskarank44', api_key='rzvg1HCKnP703X1eeJDv')
#plotly.offline.init_notebook_mode(connected=True)
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary
#there are 129 distinct categories of alocohol 
# here county wise liquor consumption is being calculated in 2015 2016 year and is checked whether consumption has increased or decresed
# command to run spark-submit volume-consumption-county.py output-1 volumeoutput


#def plot_bar_x(df_pandas):
    #plt.figure(figsize=(20,16))
    #df_pandas = df_pandas.sort_values(by='volumesoldl',ascending = False)
    #county = df_pandas['county']
    #volumesold = df_pandas['volumesoldl']
    #y_pos = np.arange(len(county))
    #plt.barh(y_pos,volumesold)
    #plt.yticks(y_pos,county,fontsize=6)
    #plt.xlabel('volume sold per county')
    #plt.title('volume spld in litres')
    #plt.show()



def main(input,outputs):
    etl_data = spark.read.option('multiline', True).parquet(input)
    #data1 = etl_data.select('categoryname').distinct()
    #data2 = data1.count()
    #print(data2)
    data1 = etl_data.groupBy('county','year').agg(functions.sum('volumesoldl').alias('volumesoldl'))
    data2 = data1.sort(functions.desc('volumesoldl'))
    #data2.show(data2.count(),False)
    data2.coalesce(1).write.csv(outputs, mode='overwrite')
    #df_pandas = data2.toPandas()
    #print(df_pandas)
    



if __name__ == '__main__':
    input = sys.argv[1]
    outputs = sys.argv[2]
    main(input,outputs)