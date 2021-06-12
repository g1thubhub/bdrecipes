import time
from sys import argv
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, col

from tutorials.module1.python.utilities.helper_python import create_session, extract_raw_records, parse_raw_warc

if __name__ == "__main__":
    sample_location = (argv[1]) if len(argv) >= 2 else '/Users/me/IdeaProjects/bdrecipes/resources/warc.sample'  # ToDo: Modify path
    spark: SparkSession = create_session(3, "Query Plans")
    spark.sparkContext.setLogLevel("TRACE")
    lang_tag_mapping = [('en', 'english'), ('pt-pt', 'portugese'), ('cs', 'czech'), ('de', 'german'), ('es', 'spanish'),
                        ('eu', 'basque'), ('it', 'italian'), ('hu', 'hungarian'), ('pt-br', 'portugese'),
                        ('fr', 'french'), ('en-US', 'english'), ('zh-TW', 'chinese')]
    lang_tag_df: DataFrame = spark.createDataFrame(lang_tag_mapping, ['tag', 'language'])
    raw_records = extract_raw_records(sample_location, spark)
    warc_records_rdd: RDD = raw_records.flatMap(parse_raw_warc)
    warc_records_df: DataFrame = warc_records_rdd.toDF() \
        .select(col('target_uri'), col('language')) \
        .filter(col('language') != '')

    aggregated = warc_records_df \
        .groupBy(col('language')) \
        .agg(count(col('target_uri'))) \
        .withColumnRenamed('language', 'tag')

    joined_df = aggregated.join(lang_tag_df, ['tag'])

    joined_df.show()
    joined_df.explain(True)
    time.sleep(10 * 60)  # Freeze for 10 minutes
