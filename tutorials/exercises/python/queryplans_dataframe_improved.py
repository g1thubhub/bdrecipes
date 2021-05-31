import time
from sys import argv
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from tutorials.module1.python.utilities.helper_python import create_session, extract_raw_records, parse_raw_warc, parse_raw_wet

if __name__ == "__main__":
    input_loc_warc = (argv[1]) if len(argv) > 2 else '/Users/me/IdeaProjects/bdrecipes/resources/CC-MAIN-20191013195541-20191013222541-00000.warc'  # ToDo: Modify path
    input_loc_wet = (argv[2]) if len(argv) > 2 else '/Users/me/IdeaProjects/bdrecipes/resources/CC-MAIN-20191013195541-20191013222541-00000.warc.wet'  # ToDo: Modify path
    spark: SparkSession = create_session(3, 'Hermeneutics Exercise DataFrame Improved')

    warc_records: RDD = extract_raw_records(input_loc_warc, spark).flatMap(lambda record: parse_raw_warc(record))
    wet_records: RDD = extract_raw_records(input_loc_wet, spark).flatMap(lambda record: parse_raw_wet(record))

    from pyspark.sql.functions import col
    warc_records_df: DataFrame = warc_records.toDF().select(col('target_uri'), col('language')) \
        .filter(col('language') == 'es')
    wet_records_df: DataFrame = wet_records.toDF().select(col('target_uri'), col('plain_text'))

    joined_df = warc_records_df.join(wet_records_df, ['target_uri'])
    print('@@ Result: ' + str(joined_df.count()))  # 133
    time.sleep(10 * 60)  # For exploring WebUI
