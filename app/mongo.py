from pymongo import MongoClient
from app.config import settings


def get_database():
    client = MongoClient(settings.mongo_url())

    return client['words']


def get_words_table():
    db_name = get_database()
    return db_name["words"]


if __name__ == "__main__":
    dbname = get_database()


def seed():
    words_table = get_words_table()
    with open("data/words.txt") as file:
        words: list[dict[str, str]] = []
        for line in file:
            for word in line.split():
                clean_word = word.strip().replace(".", "").replace(",", "").lower()
                words.append({"word": clean_word})
        words_table.insert_many(words)
