from utils.config import get_config 


def read_table_database(spark, table_name):
    config = get_config()
    df = spark.read.format('jdbc') \
    .option('url',config['database']['DB_STRING_CONNECTION']) \
    .option('dbtable',table_name) \
    .option('user',config['database']['DB_USER']) \
    .option('password', config['database']['DB_PASSWORD']) \
    .option('driver',"org.postgresql.Driver") \
    .load()

    return df