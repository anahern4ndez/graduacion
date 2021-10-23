# Databricks notebook source
# pyspark imports 
from pyspark.sql.functions import col, to_date, to_timestamp, lit, concat, date_format, sha2, concat_ws, avg, min, unbase64, when, date_trunc
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrameWriter
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.column import Column
from pyspark.sql.window import Window

# python imports
from datetime import datetime
import pytz

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load IoT data to SQL Server

# COMMAND ----------

spark = SparkSession\
    .builder\
    .appName("PythonALS")\
    .getOrCreate()

storage_container = 'iot'
storage_account_name = 'haustacc'
storage_account_access_key = 'XYUDB4MCk8NjOI3ES3fjB7d94xqcRbIlok6M6CAqUBi2I3l451UZW8y1XjDGdCng+R1hoLlcd3QXe0K4mEvslA=='
# blob_sas = 'sp=racwdlmeop&st=2021-09-15T18:38:47Z&se=2021-09-16T02:38:47Z&spr=https&sv=2020-08-04&sr=c&sig=jqVe8pECINmoeSeVVz5t5kJw5wuzgM4gpekGYh5Vl3A%3D'
# blob_sas = '?sv=2020-08-04&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-09-16T02:33:55Z&st=2021-09-15T18:33:55Z&sip=181.174.107.54&spr=https&sig=gvoe7A1Xeg05lNY4gqBF503Vll7O7rDqz3DdQBwazTo%3D'
# blob_sas = 'sp=racwdlmeop&st=2021-09-15T18:38:47Z&se=2021-09-23T02:38:47Z&sip=181.174.107.54&spr=https&sv=2020-08-04&sr=c&sig=rne9jf3eXyBDUTrryoDgVBwYCwp%2BGYMIjOltn2SuxEI%3D'
# spark.conf.set('fs.azure.sas.' + storage_container + '.' + storage_account_name + '.dfs.core.windows.net', blob_sas)
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.dfs.core.windows.net', storage_account_access_key)

# spark.conf.set(
#   "fs.azure.sas.<container-name>.<storage-account-name>.blob.core.windows.net",
#   "<complete-query-string-of-sas-for-the-container>")


# COMMAND ----------

# event hub init 
# IOT_CS = "Endpoint=sb://iothub-ns-sguptaioth-4012358-1c55ddfc30.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=LcrLjsLZKxjdzYklb4Dp2egNnKwjKLveywWUhVNIJyM=;EntityPath=sguptaiothub" # dbutils.secrets.get('iot','iothub-cs') # IoT Hub connection string (Event Hub Compatible)

IOT_CS = "Endpoint=sb://ihsuprodblres059dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=2MICIVAzdn79s/pIx3APApvYDn0ERQkIfMRMEy8tTgo=;EntityPath=iothub-ehub-hau-iot-hu-14866394-2326a03506"
ehConf = { 
  'eventhubs.connectionString':sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(IOT_CS),
  'ehName': "iothub-ehub-hau-iot-hu-14866394-2326a03506"
}

# COMMAND ----------

# Read directly from IoT Hub using the EventHubs library for Databricks

# schema = "timestamp timestamp, deviceId string, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"
schema = "TIMESTAMP timestamp, deviceId string, CO double, SO2 double, O3 double, NO2 double, temperature double, humidity double"

iot_stream = (
  spark.readStream.format("eventhubs")                                               # Read from IoT Hubs directly
  .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
  .load()                                                                          # Load the data
  .withColumn('reading', f.from_json(col('body').cast('string'), schema))        # Extract the "body" payload from the messages
  .withColumn("ms_time", f.from_utc_timestamp(f.current_timestamp(),"America/Guatemala"))
#  .withColumn("ms_time", col("reading.TIMESTAMP"))
  .withColumn("data_no2", col("reading.NO2").cast('float'))
  .withColumn("data_so2", col("reading.SO2").cast('float'))
  .withColumn("data_co", col("reading.CO").cast('float'))
  .withColumn("data_o3", col("reading.O3").cast('float'))
  .withColumn("temp", col("reading.TEMPERATURE").cast('float'))
  .withColumn("humidity", col("reading.HUMIDITY").cast('float'))
  .withColumn("sensor_id", col("reading.deviceId"))
  .fillna(0, ["data_no2", "data_so2", "data_co", "data_o3", "temp", "humidity", "sensor_id"])
  .withColumn("entry_uid_tmp", concat(col("sensor_id"), lit("_"), date_format(col("ms_time"), "ddMMyyyy.HHmmss")))
  .withColumn("entry_uid", sha2(concat_ws("_", *["entry_uid_tmp", "data_co", "data_o3", "data_no2", "data_so2", "humidity", "temp"]), 256))
)



# # Schema of incoming data from IoT hub
# schema = "timestamp timestamp, deviceId string, temperature double, humidity double, windspeed double, winddirection string, rpm double, angle double"

# # Read directly from IoT Hub using the EventHubs library for Databricks
# # iot_stream = (
# #   spark.readStream.format("org.apache.spark.sql.event<hubs.EventHubsSourceProvider")                                               # Read from IoT Hubs directly
# #     .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
# #     .load()                                                                          # Load the data
# #     .withColumn('reading', f.from_json(col('body').cast('string'), schema))        # Extract the "body" payload from the messages
# #     .select('reading.*', f.to_date('reading.timestamp').alias('date'))               # Create a "date" field for partitioning
# # )



# iot_stream = (spark.readStream.format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider")                                               # Read from IoT Hubs directly
#     .options(**ehConf)                                                               # Use the Event-Hub-enabled connect string
#     .load()  )
display(iot_stream)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Processed Data - batches de 1hr
# MAGIC 
# MAGIC ### write a base de datos SQL server

# COMMAND ----------

# connect to azure sql server 

jdbcHostname = "hau-sql-db-srv.database.windows.net"
jdbcPort = "1433"
jdbcDatabase = "hau-sql-db"
user = "hauadmin"
password = "gradUVG2021"

connection_str = "jdbc:sqlserver://hau-sql-db-srv.database.windows.net:1433;database=hau-sql-db;user=hauadmin@hau-sql-db-srv;password=gradUVG2021;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

url = "jdbc:sqlserver://hau-sql-db-srv.database.windows.net:1433;database=hau-sql-db"
sensor_data_table = "iot_data"

properties = {
 "user" : user,
 "password" : password }

# COMMAND ----------

"""
  La siguiente funcion se utiliza para hacer un writestream 
"""
#   fdf = DataFrameWriter(df)
#   fdf.jdbc(url=url, table= sensor_data_table, mode ="append", properties = properties)
def foreach_batch_func(df, batchId):
#   (df.write
#     .format("com.microsoft.sqlserver.jdbc.spark") \
#     .mode("append") \
#     .option("url", url) \
#     .option("dbtable", sensor_data_table) \
#     .option("user", user) \
#     .option("password", password) \
#     .option("forward_spark_azure_storage_credentials", "true") \
# #     .option("tempdir", "abfss://" + storage_container + "@" + storage_account_name + ".dfs.core.windows.net/tmp-stream") \
#     .save())
  fdf = DataFrameWriter(df)
  fdf.jdbc(url=url, table= sensor_data_table, mode ="append", properties = properties)

# COMMAND ----------

iot_strm_select = (iot_stream
  .withColumnRenamed("ms_time", "date_time")
  .select("sensor_id", "entry_uid", "date_time", "data_co", "data_o3", "data_no2", "data_so2", "humidity", "temp")
)

# COMMAND ----------

# escribir los datos recibidos de los sensores a la tabla de SQL server => iot_data
try:
  (iot_strm_select.writeStream \
     .outputMode("append")
#     .format("com.microsoft.sqlserver.jdbc.spark") \
#     .option("url", url) \
#     .option("dbtable", sensor_data_table) \
#     .option("user", user) \
#     .option("password", password) \
#     .outputMode("update") \
    .foreachBatch(foreach_batch_func)
    .start())
except ValueError as error :
    print("Connector write failed", error)

# COMMAND ----------

# MAGIC %md
# MAGIC # calculo de AQI para NO2, CO, O3, SO2
# MAGIC 
# MAGIC #### dimensionales para reporte de contaminantes:
# MAGIC NO2: ppb (parts per billion)
# MAGIC 
# MAGIC CO: ppm (parts per million)
# MAGIC 
# MAGIC O3: ppm (parts per million)
# MAGIC 
# MAGIC SO2: ppb (parts per billion)

# COMMAND ----------

def no2_subindex(x):
    """
      Calculo de subíndice del contaminante NO2.
      @params
        * x: medición del contaminante convertido a su dimensional de reporte (ppb).
    """
    if x <= 53:
        return ((50-0)/(53-0)) * (x - 0) + 0
    elif x <= 100:
        return ((100-51)/(100-54)) * (x - 54) + 51
    elif x <= 360:
        return ((150-101)/(360-101)) * (x - 101) + 101
    elif x <= 649:
        return ((200-151)/(649-361)) * (x - 361) + 151
    elif x <= 1249:
        return ((300-201)/(1249-650)) * (x - 650) + 201
    elif x > 1249:
        return ((400-301)/(1649-1250)) * (x - 1250) + 301
    else:
        return 0


def co_subindex(x):
    """
      Calculo de subíndice del contaminante CO.
      @params
        * x: medición del contaminante convertido a su dimensional de reporte (mg/m3).
    """
    if x <= 4.4:
        return ((50-0)/(4.4-0)) * (x - 0) + 0
    elif x <= 9.4:
        return ((100-51)/(9.4-4.5)) * (x - 4.5) + 51
    elif x <= 12.4:
        return ((150-101)/(12.4-9.5)) * (x - 9.5) + 101
    elif x <= 15.4:
        return ((200-151)/(15.4-12.5)) * (x - 12.5) + 151
    elif x <=30.4:
        return ((400-301)/(30.4-15.5)) * (x - 15.5) + 301
    elif x > 40.4:
        return ((300-201)/(40.4-30.5)) * (x - 30.5) + 201
    else:
        return 0

      
def o3_subindex(x):
    """
      Calculo de subíndice del contaminante O3.
      @params
        * x: medición del contaminante convertido a su dimensional de reporte (ug / m3).
    """
    if x <= 0.054:
        return ((50-0)/(0.054-0)) * (x - 0) + 0
    elif x <= 0.070:
        return ((100-51)/(0.070-0.055)) * (x - 0.055) + 51
    elif x <= 0.085:
        return ((150-101)/(0.085-0.071)) * (x - 0.071) + 101
    elif x <= 0.105:
        return ((200-151)/(0.105-0.086)) * (x - 0.086) + 151
    elif x > 0.105:
        return ((300-201)/(0.2-0.106)) * (x - 0.106) + 201
    else:
        return 0

      
def so2_subindex(x):
    """
      Calculo de subíndice del contaminante SO2.
      @params
        * x: medición del contaminante convertido a su dimensional de reporte (ug / m3).
    """
    if x <= 35:
        return ((50-0)/(35-0)) * (x - 0) + 0
    elif x <= 75:
        return ((100-51)/(75-36)) * (x - 36) + 51
    elif x <= 185:
        return ((150-101)/(185-76)) * (x - 76) + 101
    elif x <= 304:
        return ((200-151)/(304-186)) * (x - 186) + 151
    elif x <= 604:
        return ((300-201)/(604-305)) * (x - 305) + 201
    elif x > 604:
        return ((400-301)/(804-605)) * (x - 605) + 301
    else:
        return 0

# COMMAND ----------

def convert_measure_udf(measure, pollutant):
  """
      Convertir las medidas recibidas por los sensores a medidas con las unidades de medida utilizadas para el calculo del aqi. 
  """
  if str(pollutant) == "NO2":
    # 1 part per million = 1,000 parts per billion
    aqi_value = no2_subindex(measure)
    return int(aqi_value)
  elif str(pollutant) == "CO":
    # mg/m3 = (ppm * (g/mol)) / 24.45
    aqi_value = co_subindex(measure)
    return int(aqi_value)
  elif str(pollutant) == "SO2":
    aqi_value = so2_subindex(measure)
    return int(aqi_value)
  elif str(pollutant) == "O3":
    aqi_value = o3_subindex(measure)
    return int(aqi_value)


# COMMAND ----------

def calculate_general_aqi(p1, p2, p3, p4):
  return int(max([p1, p2, p3, p4]))

# COMMAND ----------

gt_time = datetime.now().astimezone(pytz.timezone('America/Guatemala'))
print(gt_time)

# COMMAND ----------

"""
  La siguiente funcion se utiliza para hacer un writestream 
"""
from datetime import datetime
import pytz
#   fdf = DataFrameWriter(df)
#   fdf.jdbc(url=url, table= sensor_data_table, mode ="append", properties = properties)
def foreach_batch_func2(df, batchId):
  gt_time = datetime.now().astimezone(pytz.timezone('America/Guatemala'))
  max_time_per_station = (
    df
    .withWatermark("date_time", "1 minute")
  #   .withColumn("row_num", f.row_number().over(Window.partitionBy("sensor_id").orderBy(col("date_time").desc())))
  #   .filter("row_num = 1")
    .groupBy(f.window('date_time', "1 hour", "1 hour"), "sensor_id")
    .agg(f.max("date_time"))
  #   .orderBy(col("window.start").desc())
    .withColumn("max_timestamp_trunc", date_trunc("Hour", col("max(date_time)")))
    .filter(col("max_timestamp_trunc") == date_trunc("Hour", f.from_utc_timestamp(f.current_timestamp(),"America/Guatemala"))) # aseguramos de obtener unicamente la ultima hora
  )

  iot_data_table = (
    spark
    .read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", sensor_data_table)
    .option("user", user)
    .option("password", password)
    .load()
    # filtramos las ultimas 10 horas, lo demas no nos sirve para el calculo
    .withColumn("ms_time", date_trunc("Hour", f.from_utc_timestamp(f.current_timestamp(),"America/Guatemala")) - f.expr('INTERVAL 10 HOURS'))
    .filter("date_time > ms_time")
    .withColumn("timestamp_trunc", date_trunc("Hour", col("date_time")))
    .groupBy("sensor_id", "ms_time", "timestamp_trunc")
    .agg(
      f.max('data_no2').alias("max_no2"), f.max('data_so2').alias("max_so2"),
      avg('temp').alias("temp_avg"), avg('humidity').alias("humidity_avg"),
      f.max('data_co').alias("max_co"), f.max('data_o3').alias("max_o3")

    )
    .withColumn("measure_window_start_1hr", col("timestamp_trunc") - f.expr('INTERVAL 1 HOURS'))
    .withColumn("measure_window_start_8hr", col("timestamp_trunc") - f.expr('INTERVAL 8 HOURS'))
    .orderBy(col("timestamp_trunc").desc(), "sensor_id")
    .withColumn("max_co_8hrs", f.max("max_co").over(Window.partitionBy("sensor_id")))
    .withColumn("max_o3_8hrs", f.max("max_o3").over(Window.partitionBy("sensor_id")))

    .withColumnRenamed("timestamp_trunc", "measure_window_end")
    # calculos de aqi 

    .withColumn("no2_aqi", (f.udf(convert_measure_udf)(col("max_no2"), f.lit("NO2"))).cast('float'))
    .withColumn("so2_aqi", (f.udf(convert_measure_udf)(col("max_so2"), f.lit("SO2"))).cast('float'))
    .withColumn("co_aqi", (f.udf(convert_measure_udf)(col("max_co_8hrs"), f.lit("CO"))).cast('float'))
    .withColumn("o3_aqi", (f.udf(convert_measure_udf)(col("max_o3_8hrs"), f.lit("O3"))).cast('float'))
  )
  processed_data = (
    max_time_per_station.join(iot_data_table.withColumnRenamed("sensor_id", "sid"), 
          (col("measure_window_end") == col("max_timestamp_trunc")) & (col("measure_window_start_1hr") == (col("max_timestamp_trunc") - f.expr('INTERVAL 1 HOURS')))
             & (col("sid") == col("sensor_id")), "left")

    .withColumn("general_aqi", f.udf(calculate_general_aqi) (col("no2_aqi"), col("so2_aqi"), col("co_aqi"), col("o3_aqi")))
    .withColumn("entry_uid", sha2(concat_ws("_", *["measure_window_end", "sensor_id", "co_aqi", "o3_aqi", "no2_aqi", "so2_aqi", "humidity_avg", "temp_avg"]), 256))
    .withColumn("aqi_range", 
               when((col("general_aqi") > 0) & (col("general_aqi") < 50), 1)
               .when((col("general_aqi") > 51) & (col("general_aqi") < 100), 2)
               .when((col("general_aqi") > 101) & (col("general_aqi") < 150), 3)
               .when((col("general_aqi") > 151) & (col("general_aqi") < 200), 4)
               .when((col("general_aqi") > 201) & (col("general_aqi") < 250), 5)
               .when((col("general_aqi") > 251), 6)

    )
  #   .withWatermark('measure_window_end', '59 minutes')


  ).select("measure_window_start_1hr", "measure_window_start_8hr", "measure_window_end", "co_aqi", "so2_aqi", "no2_aqi", "o3_aqi", "general_aqi", "temp_avg", "humidity_avg", "entry_uid", "sensor_id")

  fdf = DataFrameWriter(processed_data)
  fdf.jdbc(url=url, table= "processed_data", mode ="append", properties = properties)

# COMMAND ----------


iot_data_table = (
  spark
  .read
  .format("jdbc")
  .option("url", url)
  .option("dbtable", sensor_data_table)
  .option("user", user)
  .option("password", password)
  .load()
  # filtramos las ultimas 10 horas, lo demas no nos sirve para el calculo
  .withColumn("ms_time", date_trunc("Hour", f.from_utc_timestamp(f.current_timestamp(),"America/Guatemala")) - f.expr('INTERVAL 10 HOURS'))
  .filter("date_time > ms_time")
  .withColumn("timestamp_trunc", date_trunc("Hour", col("date_time")))
  .groupBy("sensor_id", "ms_time", "timestamp_trunc")
  .agg(
    f.max('data_no2').alias("max_no2"), f.max('data_so2').alias("max_so2"),
    avg('temp').alias("temp_avg"), avg('humidity').alias("humidity_avg"),
    f.max('data_co').alias("max_co"), f.max('data_o3').alias("max_o3")
    
  )
  .withColumn("measure_window_start_1hr", col("timestamp_trunc") - f.expr('INTERVAL 1 HOURS'))
  .withColumn("measure_window_start_8hr", col("timestamp_trunc") - f.expr('INTERVAL 8 HOURS'))
  .orderBy(col("timestamp_trunc").desc(), "sensor_id")
  .withColumn("max_co_8hrs", f.max("max_co").over(Window.partitionBy("sensor_id")))
  .withColumn("max_o3_8hrs", f.max("max_o3").over(Window.partitionBy("sensor_id")))
  
  .withColumnRenamed("timestamp_trunc", "measure_window_end")
  # calculos de aqi 
  
  .withColumn("no2_aqi", (f.udf(convert_measure_udf)(col("max_no2"), f.lit("NO2"))).cast('float'))
  .withColumn("so2_aqi", (f.udf(convert_measure_udf)(col("max_so2"), f.lit("SO2"))).cast('float'))
  .withColumn("co_aqi", (f.udf(convert_measure_udf)(col("max_co_8hrs"), f.lit("CO"))).cast('float'))
  .withColumn("o3_aqi", (f.udf(convert_measure_udf)(col("max_o3_8hrs"), f.lit("O3"))).cast('float'))
)

display(iot_data_table)

# COMMAND ----------

gt_time = datetime.now().astimezone(pytz.timezone('America/Guatemala'))
print(gt_time)

# COMMAND ----------

  max_time_per_station = (
    iot_strm_select
    .withWatermark("date_time", "1 minute")
  #   .withColumn("row_num", f.row_number().over(Window.partitionBy("sensor_id").orderBy(col("date_time").desc())))
  #   .filter("row_num = 1")
    .groupBy(f.window('date_time', "1 hour", "1 hour"), "sensor_id")
    .agg(f.max("date_time"))
  #   .orderBy(col("window.start").desc())
    .withColumn("max_timestamp_trunc", date_trunc("Hour", col("max(date_time)")))
    .withColumn("now_trunc", date_trunc("Hour", f.from_utc_timestamp(f.current_timestamp(),"America/Guatemala")))
#     .filter(col("max_timestamp_trunc") == date_trunc("Hour", f.lit(gt_time).cast('timestamp')))
  )
#   .groupBy("sensor_id").agg(f.max("date_time").alias("max_timestamp"))
#   .withColumn("max_timestamp_trunc", date_trunc("Hour", col("max_timestamp")))
#   .join(iot_data_table
#         .withColumnRenamed("date_time", "timestamp")
#         .withColumnRenamed("sensor_id", "sid"), (col("sensor_id") == col("sid") ) & (col("timestamp").between(col("max_timestamp_trunc"), col("max_timestamp_trunc") - f.expr('INTERVAL 8 HOURS'))), "left")
#   .withColumn("max_timestamp_trunc", date_trunc("Hour", col("max_timestamp")))
  
#   .select("sensor_id", "max_timestamp_trunc")

# max_time_per_station = (
#   iot_data_table.groupBy().agg(f.max("date_time").alias("max_timestamp"))
#   .withColumn("max_timestamp_trunc", date_trunc("Hour", col("max_timestamp")))
# )
display(max_time_per_station)

# COMMAND ----------

display(max_time_per_station)

# COMMAND ----------

processed_data = (
  max_time_per_station.join(iot_data_table.withColumnRenamed("sensor_id", "sid"), 
        (col("measure_window_end") == col("max_timestamp_trunc")) & (col("measure_window_start_1hr") == (col("max_timestamp_trunc") - f.expr('INTERVAL 1 HOURS')))
           & (col("sid") == col("sensor_id")), "left")

  .withColumn("general_aqi", f.udf(calculate_general_aqi) (col("no2_aqi"), col("so2_aqi"), col("co_aqi"), col("o3_aqi")))
  .withColumn("entry_uid", sha2(concat_ws("_", *["measure_window_end", "sensor_id", "co_aqi", "o3_aqi", "no2_aqi", "so2_aqi", "humidity_avg", "temp_avg"]), 256))
  .withColumn("aqi_range", 
             when((col("general_aqi") > 0) & (col("general_aqi") < 50), 1)
             .when((col("general_aqi") > 51) & (col("general_aqi") < 100), 2)
             .when((col("general_aqi") > 101) & (col("general_aqi") < 150), 3)
             .when((col("general_aqi") > 151) & (col("general_aqi") < 200), 4)
             .when((col("general_aqi") > 201) & (col("general_aqi") < 250), 5)
             .when((col("general_aqi") > 251), 6)

  )
#   .withWatermark('measure_window_end', '59 minutes')
  

).select("measure_window_start_1hr", "measure_window_start_8hr", "measure_window_end", "co_aqi", "so2_aqi", "no2_aqi", "o3_aqi", "general_aqi", "temp_avg", "humidity_avg", "entry_uid", "sensor_id")
  
display(processed_data)

# COMMAND ----------

:# final_df = processed_data.select("measure_window_start_1hr", "measure_window_start_8hr", "measure_window_end", "co_aqi", "so2_aqi", "no2_aqi", "o3_aqi", "general_aqi", "temp_avg", "humidity_avg", "entry_uid", "sensor_id")

# COMMAND ----------

# escribir los datos recibidos de los sensores a la tabla de SQL server => iot_data
try:
  (iot_strm_select.writeStream \
#        .outputMode("append") \
#     .format("com.microsoft.sqlserver.jdbc.spark") \
#     .option("url", url) \
#     .option("dbtable", "processed_data") \
#     .option("user", user) \
#     .option("password", password) \
   .foreachBatch(foreach_batch_func2)
   .trigger(processingTime = '1 hour')
    .start())
except ValueError as error :
    print("Connector write failed", error)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

