import time
from sys import argv
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from tutorials.module1.python.utilities.helper_python import create_session, extract_raw_records, parse_raw_warc, parse_raw_wet

if __name__ == "__main__":
    input_loc_warc = (argv[1]) if len(argv) > 2 else '/Users/me/IdeaProjects/bdrecipes/resources/CC-MAIN-20191013195541-20191013222541-00000.warc'  # ToDo: Modify path
    input_loc_wet = (argv[2]) if len(argv) > 2 else '/Users/me/IdeaProjects/bdrecipes/resources/CC-MAIN-20191013195541-20191013222541-00000.warc.wet'  # ToDo: Modify path
    spark: SparkSession = create_session(3, 'Hermeneutics Exercise RDD Improved')

    warc_records: RDD = extract_raw_records(input_loc_warc, spark).flatMap(lambda record: parse_raw_warc(record))
    wet_records: RDD = extract_raw_records(input_loc_wet, spark).flatMap(lambda record: parse_raw_wet(record))

    pair_warc: RDD = warc_records.map(lambda warc: (warc.target_uri, warc.language)) \
        .filter(lambda warc: warc[1] == 'es')
    pair_wet: RDD = wet_records.map(lambda wet: (wet.target_uri, wet.plain_text))

    joined = pair_warc.join(pair_wet)
    print('@@ Result: ' + str(joined.count()))  # 133
    time.sleep(10 * 60)  # For exploring WebUI
