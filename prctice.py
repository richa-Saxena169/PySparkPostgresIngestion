import datetime

from pyspark.sql import SparkSession
import findspark
from pyspark.sql.functions import col, lit, spark_partition_id, when

findspark.init('C:\\spark-3.1.2-bin-hadoop3.2')
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import functions as f

spark = SparkSession.builder.config("spark.jars", "project_jars/postgresql-42.3.9.jar").getOrCreate()
sc = spark.sparkContext
rdd = sc.textFile('data/taxi_zone_lookup.csv')
erdd = sc.emptyRDD()

df1 = spark.read.option('header', True).csv('data/taxi_zone_lookup.csv')
df2 = spark.read.parquet('data/yellow_tripdata_2022-06.parquet')

d1 = (df1.join(df2, df1.LocationID == df2.PULocationID, how='inner').
      withColumnRenamed('Borough', 'PickUp_Loc')
      .select('VendorID', 'passenger_count', 'fare_amount', 'DOLocationID', 'PickUp_Loc','tpep_pickup_datetime','tpep_dropoff_datetime'))

d2 = d1.join(df1, df1.LocationID == d1.DOLocationID, how='inner').\
      withColumnRenamed('Borough', 'Drop_Loc').\
      select( ( when(d1.VendorID == 1, 'Creative Mobile Technologies, LLC').
                when(d1.VendorID == 2,'VeriFone Inc.').
                when(d1.VendorID == 5,'New Corp.').otherwise('Cop Taxi')).alias('VendorName'),
                col('tpep_dropoff_datetime').alias('DropoffTime') ,col('tpep_pickup_datetime').alias('PickupTime'),
                col('passenger_count').alias('PassengerCount'),
                col('fare_amount').alias('Amount'),
                col( 'Drop_Loc').alias('DropoffLocation'), col('PickUp_Loc').alias('PickupLocation')).withColumn('loaddate',lit(datetime.datetime.now()))

#d2.show()


properties = {"user":'postgres', "password":"om", "driver": "org.postgresql.Driver"}
d2.write.mode('overwrite').jdbc(url='jdbc:postgresql://localhost:5432/postgres', table='public.NYCTripData', properties=properties)

