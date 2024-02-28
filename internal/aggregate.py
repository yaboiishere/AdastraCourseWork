from pyspark.sql import SparkSession

from app.config import Settings
from internal.words import read_words_from_mongo, aggregate_words, write_word_counts_to_pg


def count_words_in_mongo_and_persist_to_pg(spark: SparkSession, settings: Settings) -> dict[str, int]:
    records_rdd = read_words_from_mongo(spark, settings)
    word_count_rdd = aggregate_words(records_rdd)
    word_count_dict = word_count_rdd.collectAsMap()

    write_word_counts_to_pg(word_count_rdd, settings)

    return word_count_dict
