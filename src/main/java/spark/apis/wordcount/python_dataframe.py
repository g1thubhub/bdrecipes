from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import explode, split
from pyspark.sql import functions as F

# Avoids this problem: 'Exception: Python in worker has different version 2.7 than that in driver 3.6',
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.6"

if __name__ == "__main__":

    # Initialization:
    threads = 3  # program simulates a single executor with 3 cores (one local JVM with 3 threads)
    sparksession = SparkSession.builder \
        .appName("Python WordCount") \
        .master('local[{}]'.format(threads)) \
        .getOrCreate()

    # Preprocessing & reducing the input lines:
    lines = sparksession.read.text('../../../resources/moby_dick.txt')  # creates a dataframe with one column 'value'
    # creates a new column 'wpl' by creating an array of tokens with builtin SQL function 'split' and then unpacking
    # this array via builtin 'explode'; the original 'value' column is not needed anymore so it gets removed
    words_per_line = lines.select(split(F.col('value'), '\\s+').alias('wpl')).drop('value')
    words = words_per_line.select(explode(F.col('wpl')).alias('words')).drop('wpl')
    tokens = words.where(F.length(F.col('words')) > 0).select(F.lower(F.col('words')).alias('token'))  # filter empty strings & lowercase
    counts = tokens.groupBy('token').count()

    # Materializing to local disk:  coalesce is optional, without it many tiny output files are generated
    counts.coalesce(threads).write.csv('./countsplits')

    sparksession.stop()
