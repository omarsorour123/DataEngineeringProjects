from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

import logging
from utils.config import Config
# Configure logging to output to the console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Database:
    def __init__(self, config):
        self.config = config
        self.engine = self._create_engine()
        self.conn = self.engine.connect()
        self.test_connection()  # Test the connection upon initialization

    def _create_engine(self):
        # Retrieve database config from the loaded config file
        db_config = self.config['DATABASE']
        try:
            # Constructing the database connection URL
            db_url = f"postgresql://{db_config['USER']}:{db_config['PASSWORD']}@{db_config['HOST']}:{db_config['PORT']}/{db_config['DBNAME']}"
            engine = create_engine(db_url)
            logging.info("Database engine created successfully.")
            return engine
        except Exception as e:
            logging.error(f"Error while creating the database engine: {e}")
            raise

    def test_connection(self):
        """Test the database connection by executing a simple query."""
        try:
            with self.engine.connect() as connection:
                # Use the text construct for the SQL query
                result = connection.execute(text("SELECT 1"))
                logging.info("Database connection test successful.")
                for row in result:
                    logging.info(f"Test query result: {row}")
        except SQLAlchemyError as e:
            logging.error(f"Database connection test failed: {e}")

    def close(self):
        """Close the engine connection, but it's usually not necessary for SQLAlchemy's engine."""
        logging.info("Closing database engine.")
        # The engine connection is closed automatically after use with context managers


Database(config=Config('/media/sorour/8fe1c1c7-b574-4c1a-bf0c-dd93e66cb530/projects/DataEngineeringProjects/WeatherDataPipeline/config.yaml').config).test_connection()