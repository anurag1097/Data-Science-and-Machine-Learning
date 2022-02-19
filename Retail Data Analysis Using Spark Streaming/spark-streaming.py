# Importing required libraries

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Defining utility user defined functions

def get_total_item_count(items):
    """
    This method is used to calculate the toal number of items in an order.
    :param items: The list of items in an order.
    :return total_count: The sum of quantities of each item in the order.
    """
    total_count = 0
    for item in items:
        total_count = total_count + int(item['quantity'])
    return total_count

def get_total_order_cost(items, order_type):
    """
    This method is used to calculate the total cost of an order.
    :param items: The list of items in an order.
    :param order_type: This determines whether the order is a fresh one or a return.
    :return cost: This return sum of quantity * unit_price for each item inthe order. If the order is a return order, the total cost is multiplied my -1.
    """
    cost = 0
    for item in items:
        cost = cost + (int(item['quantity'])*float(item['unit_price']))
    if order_type == 'ORDER':
        return cost
    else:
        return -1*cost

def get_is_order(order):
    """
    This method is to determine whether a the order is a new buy order or not.
    :param order: Detrmines the type of order on which the value of 'is_order' column is set.
    :return 0/1: Return 1 if order is a new buy order else return 0.
    """
    if order == 'ORDER':
        return 1
    else:
        return 0

def get_is_return(order):
    """
    This method is to determine whether a the order is a return order or not.
    :param order: Detrmines the type of order on which the value of 'is_return' column is set.
    :return 0/1: Return 1 if order is a returrn order else return 0.
    """
    if order == 'RETURN':
        return 1
    else:
        return 0

# Setting up the spark session

spark = SparkSession \
        .builder \
        .appName('RetailDataAnalysis') \
        .getOrCreate()

# Setting the spark log level to ERROR.

spark.sparkContext.setLogLevel('ERROR')

# Reading data from the given kafka server and topic

orderRaw = spark.readStream \
           .format('kafka') \
           .option("kafka.bootstrap.servers","18.211.252.152:9092") \
           .option('subscribe',  'real-time-project') \
           .option("startingOffsets", "latest") \
           .load()

# Defining the json Schema of the data to read it properly.

jsonSchema = StructType() \
             .add("invoice_no", StringType()) \
             .add("country", StringType()) \
             .add("timestamp", TimestampType()) \
             .add("type", StringType()) \
             .add("items", ArrayType(StructType([
                 StructField("SKU", StringType()),
                 StructField("title", StringType()),
                 StructField("unit_price", StringType()),
                 StructField("quantity", StringType())])))

# Coverting raw data to a string and aliasing it as data.

orderStream = orderRaw.select(from_json(col('value').cast('string'), jsonSchema).alias('data')).select('data.*')

# Defining a UDF's with utility functions for calculating different column values.

# UDF to get to total items in an order.
add_total_item_count = udf(get_total_item_count, IntegerType())

# UDF to get total cost of an order.
add_total_order_cost = udf(get_total_order_cost, FloatType())

# UDF to determine if an order is an order is a new buy order.
add_is_order = udf(get_is_order, IntegerType())

# UDF to determine if an order is a return order.
add_is_return = udf(get_is_return, IntegerType())

# Calculating additional columns using the UDF's defined above.

expandedOrderStream = orderStream \
                      .withColumn("total_items", add_total_item_count(orderStream.items)) \
                      .withColumn('total_cost', add_total_order_cost(orderStream.items, orderStream.type)) \
                      .withColumn('is_order', add_is_order(orderStream.type)) \
                      .withColumn('is_return', add_is_return(orderStream.type))
                      
# Writing the summarized information to console at a processing interval of 1 minute.

extendedOrderQuery = expandedOrderStream \
                     .select("invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return") \
                     .writeStream \
                     .outputMode("append") \
                     .format("console") \
                     .option("truncate", "false") \
                     .trigger(processingTime="1 minute") \
                     .start()
                     
# Calculating Time Based Key Performance Indicators

aggStreamByTime = expandedOrderStream \
                  .withWatermark("timestamp", "1 minute") \
                  .groupBy(window("timestamp", "1 minute", "1 minute")) \
                  .agg(sum("total_cost").alias("total_sale_volume"),
                          count("invoice_no").alias("OPM"),
                          avg("is_return").alias('rate_of_return'),
                          avg("total_cost").alias('average_transaction_size'))

# Writing Time Based KPI values to a json file.

queryByTime = aggStreamByTime \
              .select("window", "OPM", 'total_sale_volume', 'rate_of_return', 'average_transaction_size') \
              .writeStream \
              .outputMode('append') \
              .format('json') \
              .option("truncate", "false") \
              .option('path', '/output/timeBasedKPI') \
              .option("checkpointLocation", "/output/timeBasedKPI") \
              .trigger(processingTime="1 minute") \
              .start()

# Calculating Time and Country Based KPI values.

aggStreamByTimeCountry = expandedOrderStream \
                         .withWatermark('timestamp', '1 minute') \
                         .groupBy(window('timestamp', '1 minute', '1 minute'), 'country') \
                         .agg(sum("total_cost").alias("total_sale_volume"),
                          count("invoice_no").alias("OPM"),
                          avg("is_return").alias('rate_of_return'))

# Writing Time and Country Based KPI values to a json file.

queryByTimeCountry = aggStreamByTimeCountry \
                     .select("window", 'country', "OPM", 'total_sale_volume', 'rate_of_return') \
                     .writeStream \
                     .outputMode('append') \
                     .format('json') \
                     .option('truncate', 'false') \
                     .option('path', '/output/timeCountryBasedKPI') \
                     .option("checkpointLocation", "/output/timeCountryBasedKPI") \
                     .trigger(processingTime = '1 minute') \
                     .start()

# Awaiting termination of the above written queries.

extendedOrderQuery.awaitTermination()
queryByTime.awaitTermination()
queryByTimeCountry.awaittermination()

