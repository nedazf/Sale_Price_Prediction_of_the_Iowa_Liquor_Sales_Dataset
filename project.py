#This file creates a correlation plot for numerical features.
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import os
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
from pyspark.sql.functions import col
assert spark.version >= '2.3' # make sure we have Spark 2.3+
from pyspark.sql import functions, types
# sc = spark.sparkContext
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from functools import reduce
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


import numpy as np
import pandas as pd
import matplotlib; matplotlib.use('Agg') # don't fail when on headless server
import matplotlib.pyplot as plt

from matplotlib import cm as cm
from matplotlib.ticker import FuncFormatter
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier, LogisticRegression
from pyspark.ml.regression import RandomForestRegressor



# add more functions as necessary


def main(inputs, outputfile, outputcorrplot, histogrampath):
    # main logic starts here
    observation_schema = types.StructType([ # commented-out fields won't be read
        types.StructField('invoicenumber', types.StringType(), True),
        types.StructField('date', types.StringType(), True),
        types.StructField('storenumber', types.IntegerType(), True),
        types.StructField('storename', types.StringType(), True),
        types.StructField('address', types.StringType(), True),
        types.StructField('city', types.StringType(), True),
        types.StructField('zipcode', types.IntegerType(), True),
        types.StructField('storelocation', types.StringType(), True),
        types.StructField('countynumber', types.IntegerType(), True),
        types.StructField('county', types.StringType(), True),
        types.StructField('category', types.IntegerType(), True),
        types.StructField('categoryname', types.StringType(), True),
        types.StructField('vendornumber', types.IntegerType(), True),
        types.StructField('vendorname', types.StringType(), True),
        types.StructField('itemnumber', types.IntegerType(), True),
        types.StructField('itemdescription', types.StringType(), True),
        types.StructField('pack', types.IntegerType(), True),
        types.StructField('bottlevolume', types.IntegerType(), True),
        types.StructField('statebottlecost', types.FloatType(), True),
        types.StructField('statebottleretail', types.FloatType(), True),
        types.StructField('bottlessold', types.IntegerType(), True),
        types.StructField('sale', types.FloatType(), True),
        types.StructField('volumesoldl', types.FloatType(), True),
        types.StructField('volumesoldg', types.FloatType(), True) ,])

    etl_data = spark.read.option('multiline', True).csv(inputs, schema = observation_schema)
    #etl_data.show()
    etl_data = etl_data.withColumn('month',functions.split(('date'),'/').getItem(0))
    etl_data = etl_data.withColumn('day',functions.split(('date'),'/').getItem(1))
    etl_data = etl_data.withColumn('year',functions.split(('date'),'/').getItem(2))
    etl_data = etl_data.withColumn('latlong',functions.split(('storelocation'),"\n").getItem(2))
    etl_data = etl_data.withColumn('latitude',functions.split(('latlong'),',').getItem(0))
    etl_data = etl_data.withColumn('longitude',functions.split(('latlong'),',').getItem(1))
    #etl_data = etl_data.select('latitude',functions.regexp_replace(functions.col('latitude'),'[(,)]','').alias('latitude1'))
    etl_data = etl_data.withColumn('latitude',functions.regexp_replace(functions.col('latitude'),'[(,)]',''))
    etl_data = etl_data.withColumn('longitude',functions.regexp_replace(functions.col('longitude'),'[(,)]',''))
    etl_data = etl_data.withColumn("latitude", etl_data["latitude"].cast("float"))
    etl_data = etl_data.withColumn("longitude", etl_data["longitude"].cast("float"))
    #etl_data.createOrReplaceTempView("etl_data")
    #etl_data.show()
    etl_data = etl_data.filter(etl_data.invoicenumber.isNotNull())
    etl_data = etl_data.filter(etl_data.vendornumber.isNotNull())
    #etl_data.createOrReplaceTempView('etl_data')
    etl_data = etl_data.filter(etl_data.categoryname.isNotNull())
    etl_data = etl_data.filter(etl_data.invoicenumber.isNotNull())
    etl_data = etl_data.filter(etl_data.sale.isNotNull())
    etl_data = etl_data.withColumn('county',functions.lower(functions.col('county')))
    etl_data = etl_data.filter(etl_data.countynumber.isNotNull())
    # etl_data.write.parquet(output, mode='overwrite')

    #The line below is supposed to remove any row which ha a null value
    etl_data = etl_data.filter(~reduce(lambda x, y: x | y, [etl_data[c].isNull() for c in etl_data.columns]))

    #labels=["storenumber","zipcode", "countynumber" , "category" , "vendornumber", "itemnumber" , "pack", "bottlevolume" , "statebottlecost", "bottlessold", "sale", "volumesoldl", "volumesoldg"]
    #etl_data_num=  etl_data.select("storenumber","zipcode", "countynumber" , "category" , "vendornumber", "itemnumber" , "pack", "bottlevolume" , "statebottlecost", "bottlessold", "sale", "volumesoldl", "volumesoldg")
    histogramplots(etl_data , histogrampath)

    traintest(etl_data, outputfile)


    labels=[ "vendornumber", "itemnumber" , "pack", "bottlevolume" , "statebottlecost", "bottlessold", "sale", "volumesoldl", "volumesoldg","latitude","longitude"]
    etl_data_num=  etl_data.select( "vendornumber", "itemnumber" , "pack", "bottlevolume" , "statebottlecost", "bottlessold", "sale", "volumesoldl", "volumesoldg","latitude","longitude")

    df = etl_data_num.toPandas() #converting to pandas dataframe, might be slow for larger dataframes!

    correlation_plot(df, labels, outputcorrplot)




def traintest(data , outputfile) :
    print ('in traintest  ***********************************')
    print ('\n')
    train, test = data.randomSplit([0.7, 0.3])
    train = train.cache()

    test= test.cache()
    #first cross validation to find hyperparameters. hyperparameters with less error
    #learn on all train data to find parameters
    rf_regressor = RandomForestRegressor( featuresCol = "features" , labelCol ="sale", seed = 40)
    myFeatures = [ "pack", "bottlessold","volumesoldl", "volumesoldg","latitude","longitude"]
    # ,"vendornumber","bottlevolume","statebottlecost"
    assembler = VectorAssembler(inputCols=myFeatures, outputCol="features")
    # "vendornumber","bottlevolume","itemnumber"
    #"statebottlecost",
    pipeline = Pipeline(stages=[assembler, rf_regressor])
    rf= RandomForestRegressor()

    paramGrid = ParamGridBuilder().addGrid(rf_regressor.maxDepth, [2,5,10,15]).addGrid(rf_regressor.minInfoGain, [0.01]).addGrid(rf_regressor.numTrees, [20,30,100]).build()
    #paramGrid = ParamGridBuilder().addGrid(rf_regressor.maxDepth, [2]).addGrid(rf_regressor.minInfoGain, [0.01]).addGrid(rf_regressor.numTrees, [5]).build()


    # Run cross-validation, and choose the best set of parameters.
    crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=paramGrid,evaluator=RegressionEvaluator(predictionCol='prediction', labelCol='sale',
                                                                                                             metricName='rmse'),numFolds=8)  # use 3+ folds in practice
    # Run cross-validation, and choose the best set of parameters.
    cvModel = crossval.fit(train)
    #model = pipeline.fit(train)
    predictions = cvModel.transform (test)

    r2_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='sale',
                                       metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='sale',
                                         metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)


    print('r2 =', r2)
    print('rmse =', rmse)
    output = open (outputfile, "w")
    output.write("r2="+str(r2)+"\n")
    output.write("rmse="+str(rmse)+"\n")




def histogramplots(df, histogrampath):
    if not os.path.exists(histogrampath):
        os.makedirs(histogrampath)
    dflength= df.count()
    for x in [ "pack", "bottlessold","volumesoldl", "volumesoldg","latitude","longitude", "sale"]:
        fig, ax = plt.subplots()
        data, counts = df.select(x).rdd.flatMap(lambda x: x).histogram(dflength+1 )
        plt.hist(data[:-1], bins=20, weights=counts,color ='plum' ,  edgecolor='black', linewidth=1.2)
        plt.xlabel(x)
        #if "sold" in x:
        ax.set_yscale('log')
        plt.ylabel("Count")
        plt.savefig(histogrampath+"/"+x+".png", bbox_inches='tight')




def correlation_plot(df ,labels, outputcorrplot):

    coeff = df.corr()
    row_labels = column_labels = labels
    fig, ax = plt.subplots()
    pc = ax.pcolor(coeff, cmap=plt.cm.PiYG)
    ax.set_xticks(range(len(row_labels)))
    ax.set_yticks(range(len(row_labels)))
    plt.gca().set_xlim((0, len(labels)))
    plt.gca().set_ylim((0, len(labels)))
    fontsize=7
    ax.set_yticklabels(column_labels, fontsize=fontsize)
    ax.set_xticklabels(row_labels, rotation='vertical', fontsize=fontsize)
    for axis in [ax.xaxis, ax.yaxis]:
        axis.set(ticks=np.arange(0.5, len(labels)), ticklabels=labels)
    plt.colorbar(pc)
    plt.tight_layout()
    plt.savefig(outputcorrplot+".png", bbox_inches="tight")


if __name__ == '__main__':
    inputs = sys.argv[1]
    outputfile = sys.argv[2]
    outputcorrplot = sys.argv[3]
    histogrampath = sys.argv[4]
    main(inputs, outputfile, outputcorrplot, histogrampath)
