from pyspark.sql import 

AWS_ACCESS_KEY_ID = 'SECRET'
AWS_SECRET_ACCESS_KEY = 'SECRET'
S3_INPUT_PATH = 's3a://cloudcomput/js-youtube.txt'
S3_OUTPUT_PATH = 's3a://cloudcomput/output/'

spark = SparkSession.builder \
    .appName("WordCount") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)

text_file = spark.sparkContext.textFile(S3_INPUT_PATH)
counts = text_file.flatMap(lambda line: line.split()) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(S3_OUTPUT_PATH)
spark.stop()