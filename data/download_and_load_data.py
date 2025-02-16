from datasets import load_dataset
from sqlalchemy import create_engine, Column, String, Float, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError

import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.env_config import db_properties

ds = load_dataset("egecandrsn/weatherdata")

Base = declarative_base()
class Weather(Base):
    __tablename__ = 'weather_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    datetime = Column(String)
    tempmax = Column(Float)
    tempmin = Column(Float)
    temp = Column(Float)
    feelslikemax = Column(Float)
    feelslikemin = Column(Float)
    feelslike = Column(Float)
    dew = Column(Float)
    humidity = Column(Float)
    precib = Column(Float)
    windspeed = Column(Float)
    cloudcover = Column(Float)
    severerisk = Column(Float)

engine = create_engine(f"postgresql+psycopg2://{db_properties['db_user']}:{db_properties['db_password']}\
                        @{db_properties['db_host']}:{db_properties['db_port']}/{db_properties['db_name']}")

try:
    with engine.connect() as connection:
        print("Successfully connected to PostgreSQL!")
except OperationalError as e:
    print(f"Error connecting to PostgreSQL: {e}")

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

for record in ds['train']:
    weather = Weather(
        name=record.get("name", None),
        datetime=record.get("datetime", None),
        tempmax=record.get("tempmax", None),
        tempmin=record.get("tempmin", None),
        temp=record.get("temp", None),
        feelslikemax=record.get("feelslikemax", None),
        feelslikemin=record.get("feelslikemin", None),
        feelslike=record.get("feelslike", None),
        dew=record.get("dew", None),
        humidity=record.get("humidity", None),
        precib=record.get("precib", None),
        windspeed=record.get("windspeed", None),
        cloudcover=record.get("cloudcover", None),
        severerisk=record.get("severerisk", None),
    )
    session.add(weather)

session.commit()
session.close()