from typing import List

from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session

from app.config import config, settings
from app.database import engine, Base, get_db
from app.mongo import get_words_table, seed
from app.schema import WordBase
from internal.aggregate import count_words_in_mongo_and_persist_to_pg
from models.word import Word

Base.metadata.create_all(bind=engine)
app = FastAPI()
spark_session = config.spark_session
mongo_session = get_words_table()


@app.get("/", response_model=list[WordBase])
async def root(db: Session = Depends(get_db)):
    resp = db.query(Word).all()
    print(resp, type(resp), "This is the response")
    return resp


@app.post("/db")
async def seed_db():
    seed()
    return {"message": "Database seeded"}


@app.delete("/db")
async def delete_db(db: Session = Depends(get_db)):
    db.query(Word).delete()
    db.commit()
    mongo_session.delete_many({})
    return {"message": "Database deleted"}


@app.delete("/db/reset")
async def reset_db():
    mongo_session.delete_many({})
    seed()
    return {"message": "Database reset"}


@app.post("/aggregate", response_model=dict[str, int])
async def aggregate():
    try:
        word_counts = count_words_in_mongo_and_persist_to_pg(spark_session, settings)
        return word_counts
    except ValueError as e:
        if "RDD is empty" in str(e):
            raise HTTPException(status_code=404, detail="No words found in MongoDB")
        else:
            raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
