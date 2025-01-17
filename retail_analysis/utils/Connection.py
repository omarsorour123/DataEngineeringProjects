import psycopg2
from sqlalchemy import create_engine


def create_connection():
    try:
        engine = create_engine('postgresql+psycopg2://postgres:omar@localhost:5432/postgres')

        return engine
    except Exception as e:
        print("Error:", e)
