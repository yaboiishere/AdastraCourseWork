import os

from pydantic_settings import SettingsConfigDict, BaseSettings
from pyspark.sql import SparkSession


class Settings(BaseSettings):
    app_name: str = "Adastra Course Work"
    pg_user: str
    pg_password: str
    pg_database: str
    pg_port: int
    pg_host: str
    spark_url: str
    mongo_user: str
    mongo_pass: str
    mongo_host: str = "mongodb"
    mongo_port: int = 27017

    model_config = SettingsConfigDict(env_file="../.env")

    def database_url(self) -> str:
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}"

    def database_url_no_auth(self) -> str:
        return f"postgresql://{self.pg_host}:{self.pg_port}/{self.pg_database}"

    def database_credentials(self) -> dict:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "user": self.pg_user,
            "password": self.pg_password,
            "database": self.pg_database
        }

    def mongo_url(self) -> str:
        return f"mongodb://{self.mongo_user}:{self.mongo_pass}@{self.mongo_host}:{self.mongo_port}"


class Config:
    spark_session = None

    def __init__(self, s: Settings):
        self.spark_session = (SparkSession.builder.appName(s.app_name)
                              .master(s.spark_url)
                              .config("spark.jars.packages",
                                      "org.mongodb.spark:mongo-spark-connector:10.0.3,org.postgresql:postgresql:42.2.23"
                                      )
                              .config("spark.mongodb.input.uri", s.mongo_url())
                              .config("spark.mongodb.output.uri", s.mongo_url())
                              .getOrCreate())


settings = Settings()
config = Config(settings)
