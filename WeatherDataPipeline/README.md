
# Weather Data ETL Pipeline

## Overview
A robust ETL (Extract, Transform, Load) pipeline that collects, processes, and analyzes weather data from multiple cities. Built with Apache Airflow for orchestration, this pipeline fetches hourly weather data, applies transformations, and stores both raw and processed data for analysis.

## Features
- **Automated Data Collection**: Hourly weather data collection from OpenWeatherMap API
- **Data Transformation**: Comprehensive data processing including:
  - Temperature conversions (Celsius to Fahrenheit)
  - Wind speed conversions (m/s to mph)
  - Weather severity calculations
  - Time-based analytics
- **Data Quality**: Built-in data validation and error handling
- **Scalable Architecture**: Designed to handle multiple cities and data points
- **Monitoring**: Detailed logging and error tracking

## Project Structure
```bash
weather-etl/
├── airflow/
│   └── dags/
│       └── weather_data_etl.py
├── sql/
│   ├── table_schema.sql
│   
├── utils/
│   ├── __init__.py
│   ├── config.py
│   ├── database.py
│   ├── etl.py
│   └── schema.py
├── config.yaml
├── requirements.txt
└── README.md
```

## Technical Stack
- **Python 3.12+**
- **Apache Airflow**: Workflow orchestration
- **PostgreSQL**: Data storage
- **SQLAlchemy**: Database ORM
- **Pandas**: Data transformation
- **OpenWeatherMap API**: Weather data source

## Prerequisites
1. Python 3.12 or higher
2. PostgreSQL database
3. OpenWeatherMap API key
4. Apache Airflow installation

## Installation

1. Clone the repository
```bash
git clone https://github.com/your-username/weather-etl.git
cd weather-etl
```

2. Create and activate virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Set up config.yaml variables



## Running the Pipeline

### Using Airflow
1. Start the Airflow webserver:
```bash
airflow webserver -p 8080
```

2. Start the Airflow scheduler:
```bash
airflow scheduler
```

3. Access the Airflow UI at `http://localhost:8080`

4. Enable the `weather_data_etl` DAG

### Manual Execution
```bash
python main.py
```

## Pipeline Components

### 1. Extraction (`extract.py`)
- Fetches weather data from OpenWeatherMap API
- Handles rate limiting and API errors
- Stores raw data in staging table

### 2. Transformation (`transform.py`)
- Converts temperature units
- Calculates weather severity
- Adds time-based features
- Performs data validation

### 3. Loading (`load.py`)
- Saves processed data to CSV
- Handles file operations
- Implements error handling

## Data Schema

### Staging Table
- city (VARCHAR)
- temperature (DECIMAL)
- humidity (INTEGER)
- pressure (INTEGER)
- wind_speed (DECIMAL)
- description (VARCHAR)
- timestamp (TIMESTAMP)
- extraction_timestamp (TIMESTAMP)

### Processed Table
- Additional calculated fields
- Transformed metrics
- Analysis-ready format

## Monitoring and Maintenance

### Logs
- Located in `airflow/logs/`
- Contains detailed execution information
- Error tracking and debugging

### Error Handling
- Automatic retries for failed tasks
- Error notifications
- Data validation checks

## Contributing
1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Future Improvements
- [ ] Add more weather data sources
- [ ] Implement real-time monitoring
- [ ] Add data quality metrics
- [ ] Create visualization dashboard
- [ ] Add unit tests
- [ ] Implement data archiving

## License
This project is licensed under the MIT License - see the LICENSE file for details

## Acknowledgments
- OpenWeatherMap API for weather data
- Apache Airflow community
- All contributors to this project
