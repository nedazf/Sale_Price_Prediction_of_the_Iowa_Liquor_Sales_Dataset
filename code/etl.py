import sys
import json
import re
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('example code').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary
#spark-submit etl.py Iowa_Liquor_Sales.csv output1


def main(inputs,output):
    observation_schema = types.StructType([
        types.StructField('invoicenumber', types.StringType(), True),
        types.StructField('date', types.StringType(), True),
        types.StructField('storenumber', types.IntegerType(),True ),
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
        types.StructField('volumesoldl', types.FloatType(),True),
        types.StructField('volumesoldg', types.FloatType(), True),
        ])

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
    #etl_data.createOrReplaceTempView("etl_data")
    #etl_data.show()
    etl_data1 = etl_data.filter(etl_data.invoicenumber.isNotNull())
    etl_data1 = etl_data1.filter(etl_data1.vendornumber.isNotNull())
    #etl_data1.createOrReplaceTempView('etl_data1')
    etl_data1 = etl_data1.filter(etl_data1.categoryname.isNotNull())
    etl_data1 = etl_data1.filter(etl_data1.invoicenumber.isNotNull())
    etl_data1 = etl_data1.filter(etl_data1.sale.isNotNull())
    etl_data1 = etl_data1.withColumn('county',functions.lower(functions.col('county')))
    etl_data1 = etl_data1.filter(etl_data1.countynumber.isNotNull())
    etl_data1.write.parquet(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)