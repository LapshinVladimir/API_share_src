from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    dbname: str = Field(alias='DB_NAME')
    user: str = Field(alias='DB_USER')
    password: str = Field(alias='DB_PASSWORD')
    host: str = Field(alias='DB_HOST')
    port: str = Field(alias='DB_PORT')
    es_host: str = Field(alias='ES_HOST')
