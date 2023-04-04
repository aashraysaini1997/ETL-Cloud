# Databricks notebook source
access_key = "AKIAW4K7ZNUVWH4YSNN4"
secret_key = "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"

sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
encoded_secret_key = secret_key.replace("/", "%2F")

# COMMAND ----------

dbutils.notebook.run('/Users/aashrays@andrew.cmu.edu/kafka_loader',timeout_seconds=86400)

# COMMAND ----------

aws_bucket_name = "aashraybucket"
mount_name = "mount_s3"
dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

df = spark.read.format("csv").load("/mnt/mount_s3/",header=True)
df.count()

# COMMAND ----------

df.createOrReplaceTempView('df')

# COMMAND ----------

df_ratings = spark.sql('''select split(value,",")[0] as timestamp
,split(value,",")[1] as user_id
,split(split(split(value,",")[2],"/")[2],"=")[0] as movie_id 
,split(split(split(value,",")[2],"/")[2],"=")[1] as rating 
from df''')

# COMMAND ----------

redshift_url = "jdbc:redshift://new-cluster.473184169259.us-west-2.redshift-serverless.amazonaws.com/dev?ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory"
driverClass = "com.amazon.redshift.jdbc.Driver"

df_ratings.write \
    .format("com.databricks.spark.redshift") \
    .option("url", redshift_url) \
    .option("dbtable", "ratings") \
    .option("tempdir", "s3a://aashraybucket/") \
    .option("user", "admin") \
    .option("password", "Ansh1997!") \
    .option("aws_access_key_id", access_key) \
    .option("aws_secret_access_key", secret_key) \
    .option("forward_spark_s3_credentials", True) \
    .option("driver", driverClass) \
    .mode("overwrite") \
    .save()
