import socket
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, TimestampType, StructType, StructField

minio_socket = socket.gethostbyname('minio')

S3A_BUCKET = "kueski-store"
IN_PATH_RAW_DATA = f's3a://{S3A_BUCKET}/raws/rating.csv'
OUT_PATH_FEATURES_ALL_ROWS = f's3a://{S3A_BUCKET}/features/allrows/'
OUT_PATH_FEATURES_ONLINE_STORE = f's3a://{S3A_BUCKET}/features/onlinestore/'


spark = SparkSession \
    .builder \
    .appName("kueski_challenge") \
    .config('spark.hadoop.fs.s3a.endpoint', 'http://'+minio_socket+':9000') \
    .config("spark.hadoop.fs.s3a.access.key", "kueski") \
    .config("spark.hadoop.fs.s3a.secret.key", "kueski123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .getOrCreate()

custom_schema = StructType(
    [StructField('userId', IntegerType(), True),
    StructField('movieId', IntegerType(), True),
    StructField('rating', FloatType(), True),
    StructField('timestamp', TimestampType(), True)]
)

df = spark.read.csv(IN_PATH_RAW_DATA, header=True, schema=custom_schema)

window_def = (Window.partitionBy('userId').orderBy('timestamp'))

df_calc = df.withColumn(
    'nb_previous_ratings',
    F.row_number().over(window_def)-1
).withColumn(
    'avg_ratings_previous',
    F.avg(F.lag(df.rating).over(window_def)).over(window_def)
)

df_calc.write.mode("overwrite").parquet(OUT_PATH_FEATURES_ALL_ROWS)

# filter value to online store
w = Window.partitionBy('userId')
df_calc = df_calc.select(
    'userId',
    'nb_previous_ratings',
    'avg_ratings_previous'
).withColumn('max_nb_previous_ratings', F.max('nb_previous_ratings').over(w))\
    .where(F.col('nb_previous_ratings') == F.col('max_nb_previous_ratings'))\
    .drop('max_nb_previous_ratings')

df_calc.write.mode("overwrite").parquet(OUT_PATH_FEATURES_ONLINE_STORE)