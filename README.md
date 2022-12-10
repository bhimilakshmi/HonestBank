
CSV file stream demo

1) Download dependencies
pip install -r requirements.txt

2) Genearete sample ticker data.
   Fdr this downloaded data from yfinance and stored local temp dir .csv files
   download:
   df = yf.download(tickers=ticker, start=start, end=end, period='1d', interval='1d')
 
3) Added tickerid and tickername store in csv files:
        data['ticker'] = ticker_id
        data['ticker_name'] = row['Name']
        if not os.path.isdir(path):
            os.makedirs(path)
        data.to_csv(path+"\\"+ticker_id+".csv"
4) Execute sql scripits:
        DROP DATABASE IF EXISTS honest_bank;
        CREATE DATABASE  honest_bank;
        USE  honest_bank;
        CREATE  TABLE  tickers(
            `index` INTEGER ,
             date TIMESTAMP NOT NULL,
             ticker VARCHAR(100),
             ticker_name VARCHAR(100),
             open DOUBLE NOT NULL,
             high DOUBLE NOT NULL,
             low DOUBLE NOT NULL,
             close DOUBLE NOT NULL,
             `adj close` DOUBLE NOT NULL,
             volume INTEGER NOT NULL
       );
5) Store csv data into sql:
     for i, row in data.iterrows():
         sql = "INSERT INTO `tickers` (`" + cols + "`) VALUES (" + "%s," * (len(row) - 1) + "%s)"
         time.sleep(threadsleep)
         print(tuple(row))
         cursor.execute(sql, tuple(row))
         # the connection is not autocommitted by default, so we must commit to save our changes
        connection.commit()
6) Publish  to kafka for live streame from sql 
    Convert sql row to json and produce each row and push events topic
          
    1)convert sql rows to json list:   
      header = [i[0] for i in cursor.description]
      ticker_results_list = []
      for row in ticker_results:
         ticker = {}
         for prop, val in zip(header, row):
             ticker[prop] = val
             ticker_results_list.append(ticker)
              return json.dumps(ticker_results_list,default=json_serial)
    2)Produce data to kafka
        for ticker in datalist:
           print(ticker)
           ticker_message = json.dumps(ticker, indent=2).encode('utf-8')
           producer.produce(topic, value=ticker_message)
           producer.flush()
7)Consume live stream data in Spark sreaming:
   kafka_df = spark.readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers", "localhost:9092,localhost:9091") \
   .option("failOnDataLoss", "false") \
   .option("subscribe", "events") \
   .option("includeHeaders", "true") \
   .option("startingOffsets", "earliest") \
   .option("spark.streaming.kafka.maxRatePerPartition", "50") \
  .load()
        
   Apply schema for live stream:
      
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
    kafkaStringDF = kafka_df.selectExpr("CAST(value AS STRING)")
    kafkaDF = kafkaStringDF.select(from_json(col("value"), schema).alias('data')).select("data.*") 
7)Spark stream continouly check the counts:
     kafkaDF_new = kafkaDF.groupby('ticker',window("timestamp", "2 minutes", "1 minutes")).count()
     kafkaDF_new \
         .writeStream\
         .format("console")\
         .outputMode("complete")\
         .start()\
         .awaitTermination()

Steps to verify:
1)Mysql should be up and running
2)Kafka nedd to be up and running

Running steps:
1)start down and store data in sql:
python download_store_sql_tickers_data.py
2)push to kafka from mysql
python pust_to_kakfa_from_mysql.py
3)Run spark streaming
python kafka_pyspark.py


   