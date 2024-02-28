from pydantic import BaseModel


class WordBase(BaseModel):
    word: str
    count: int

    class Config:
        orm_mode = True


class WordCreate(WordBase):
    class Config:
        orm_mode = True
