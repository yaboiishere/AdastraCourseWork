import psycopg2
from pyspark import RDD, Row
from pyspark.sql import SparkSession

from app.config import Settings


def aggregate_words(records_rdd: RDD) -> RDD[tuple[str, int]]:
    return (records_rdd
            .map(lambda x: (x["word"], 1))
            .reduceByKey(lambda a, b: a + b))


def read_words_from_mongo(spark: SparkSession, settings: Settings) -> RDD[Row]:
    return (spark.read.format("mongodb")
            .option("database", "words")
            .option("collection", "words")
            .option("connection.uri", settings.mongo_url())
            .load().rdd)


def write_word_counts_to_pg(word_count_rdd: RDD[tuple[str, int]], settings: Settings) -> None:
    credentials = settings.database_credentials()

    def process_row(row, dbc_merge):
        dbc_merge.execute(
            "INSERT INTO words (word, count) VALUES (%s, %s) ON CONFLICT (word) DO UPDATE SET count = EXCLUDED.count",
            (row[0], row[1])
        )

    def process_partition(partition):
        db_conn = psycopg2.connect(**credentials)

        dbc_merge = db_conn.cursor()

        for row in partition:
            process_row(row, dbc_merge)

        db_conn.commit()
        dbc_merge.close()
        db_conn.close()
    (word_count_rdd.toDF(["word", "count"]).foreachPartition(process_partition))
    # .jdbc(
    #     url=f"jdbc:{settings.database_url_no_auth()}",
    #     table="words", mode="append",
    #     properties={
    #         "user": settings.pg_user,
    #         "password": settings.pg_password,
    #         "driver": "org.postgresql.Driver"}))
