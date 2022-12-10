from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
scala_version = '2.12'
spark_version = '3.3.1'
# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}'
]
#./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 kafka_pyspark.py

#function to calculate number of seconds from number of days
days = lambda i: i * 86400

spark = SparkSession \
        .builder \
        .appName("test") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .config("spark.jars.packages", ",".join(packages))\
        .getOrCreate()


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092,localhost:9091") \
    .option("failOnDataLoss", "false") \
    .option("subscribe", "events") \
    .option("includeHeaders", "true") \
    .option("startingOffsets", "earliest") \
    .option("spark.streaming.kafka.maxRatePerPartition", "50") \
    .load()
kafka_df.printSchema()
schema =  StructType()\
            .add('date',DateType())\
            .add('ticker',StringType())\
            .add('ticker_name', StringType())\
            .add('open', DoubleType())\
            .add('high',DoubleType())\
            .add('low', DoubleType()) \
            .add('close', DoubleType()) \
            .add('adj close', DoubleType()) \
            .add('volume', IntegerType())
kafkaStringDF = kafka_df.selectExpr("CAST(value AS STRING)","timestamp")
kafkaStringDF.printSchema()
kafkaDF = kafkaStringDF.select(from_json(col("value"), schema).alias('data'),"timestamp").select("data.*","timestamp")
#days2_window  = Window.partitionBy(col('ticker')).orderBy(col("date").cast('long')).rangeBetween(-days(1), 0)
#days2_window_df = kafkaDF.withColumn('rolling_average', avg("close").over(days2_window))
kafkaDF.printSchema()
kafkaDF_new = kafkaDF.groupby('ticker',window("timestamp", "2 minutes", "1 minutes")).count()
kafkaDF_new \
    .writeStream\
    .format("console")\
    .outputMode("complete")\
    .start()\
    .awaitTermination()
