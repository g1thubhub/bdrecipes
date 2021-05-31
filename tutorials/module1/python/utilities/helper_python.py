from collections import defaultdict
from typing import Tuple, DefaultDict, List
from pyspark.sql import SparkSession
import re
from tutorials.module1.python.utilities.domain_objects import WetRecord, WarcRecord

# Utility functions for bdrecipes project
# @author Phil, https://github.com/g1thubhub

blank_line = "(?m:^(?=[\r\n]))"
blank_line_regex = re.compile(blank_line)
new_line = "(\\r?\\n)+"
new_line_regex = re.compile(new_line)


def create_session(num_threads: int = 2, name: str = "Spark Application") -> SparkSession:
    session: SparkSession = SparkSession.builder \
        .master('local[{}]'.format(num_threads)) \
        .appName(name) \
        .getOrCreate()
    return session  # program simulates a single executor with numThreads cores (one local JVM with numThreads threads)


def get_neighbours(line: str):
    tokens = re.split('\\W+', line)
    return list(map(lambda token: (token, len(tokens)), tokens))


def calc_average(word_stats: Tuple[str, Tuple[int, int]]) -> Tuple[str, int]:
    word = word_stats[0]
    count = word_stats[1][0]
    neighbours = word_stats[1][1]
    average = neighbours / count
    return word, average


#################################################################################################################


def extract_raw_records(warc_loc: str, session: SparkSession):
    hadoop_confi = {"textinputformat.record.delimiter": "WARC/1.0"}
    warc_records = session.sparkContext \
        .newAPIHadoopFile(path=warc_loc, inputFormatClass="org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                          keyClass="org.apache.hadoop.io.LongWritable", valueClass="org.apache.hadoop.io.Text",
                          conf=hadoop_confi)
    return warc_records.map(lambda pair: pair[1])


def extract_meta_info(raw_meta: str) -> DefaultDict[str, str]:
    meta_entries = defaultdict(str)
    fields = new_line_regex.split(raw_meta)
    for field in fields:
        key_value = field.split(':')
        meta_key = key_value[0].strip()
        if len(meta_key) == 0:
            continue
        meta_entries[meta_key] = ':'.join(key_value[1:len(key_value)]).strip()
    return meta_entries


def extract_response_meta(response_meta: str):
    fields = new_line_regex.split(response_meta)
    content_type = ''
    language = ''
    content_length = -1
    for field in fields:
        if field.startswith('Content-Type:'):
            content_type = field[14:].strip()
        elif field.startswith('Content-Language:'):
            language = field[17:].strip()
        elif field.startswith('Content-Length:'):
            content_length = int(field[15:].strip())
    return content_type, language, content_length


def parse_raw_warc(text: str) -> Tuple[WarcRecord]:
    match = blank_line_regex.findall(text)
    if len(match) == 0:
        return ()
    else:
        match_iter = re.finditer(blank_line_regex, text)
        match_starts: List[int] = [m.end(0) for m in match_iter]
        doc_start = match_starts[0]  # start of record
        meta_boundary = match_starts[1]  # end of meta section
        response_boundary = match_starts[2]  # end of response meta section
        raw_meta_info = text[doc_start:meta_boundary].strip()
        meta_pairs: DefaultDict[str, str] = extract_meta_info(raw_meta_info)
        response_meta = text[meta_boundary + 1: response_boundary].strip()
        response_meta_triple: Tuple[str, str, int] = extract_response_meta(response_meta)
        page_content = text[response_boundary + 1:].strip()
        page_content = re.sub(new_line_regex, ' ', page_content)
        return WarcRecord(meta_pairs, response_meta_triple, page_content),


def parse_raw_wet(text: str) -> Tuple[WetRecord]:
    match = blank_line_regex.findall(text)
    if len(match) == 0:
        return ()
    else:
        match_iter = re.finditer(blank_line_regex, text)
        match_starts: List[int] = [m.end(0) for m in match_iter]
        doc_start = match_starts[0]  # start of record
        boundary = match_starts[1]  # end of meta section
        raw_meta_info = text[doc_start:boundary].strip()
        meta_pairs: DefaultDict[str, str] = extract_meta_info(raw_meta_info)
        page_content = text[boundary + 1:].strip()
        page_content = re.sub(new_line_regex, ' ', page_content)
        return WetRecord(meta_pairs, page_content),
