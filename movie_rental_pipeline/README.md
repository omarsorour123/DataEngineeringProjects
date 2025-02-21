# Movie Rental Data Analysis Project

This project is a data analysis pipeline for a movie rental database using PySpark. It performs several analyses, such as:

- **Store Revenue Analysis:** Aggregates total revenue per store per year and generates a bar chart.
- **Actor Revenue Analysis:** Retrieves total revenue per actor for a specified year.
- **Top Movie by Month:** Identifies the film with the highest total revenue for each month (and year).

## Requirements

- Python 3.x (tested on Python 3.10)
- PySpark
- Pandas
- Matplotlib
- JDBC driver for your database (configured via `utils/config.py`)

Install the required Python packages using:

```bash
pip install -r requirements.txt
```

## Configuration

The project relies on a configuration file (accessed via `utils/config.py`) that provides necessary settings such as:
- Database connection details
- Path to the JDBC driver

Make sure these settings are correctly configured for your environment.

## Running the Project
### Firstly run the scripts in init_scripts into your database
To execute the analysis, simply run the `main.py` script:

```bash
python main.py
```

When you run the project, it will:

1. **Analyze Store Revenue:**  
   - Aggregate revenue per store per year.
   - Generate and display a bar chart comparing stores' revenues over the years.
   
2. **Get Actor Revenue for 2005:**  
   - Retrieve and display a table of actors along with their total revenue for the year 2005.
   
3. **Get Top Movie Each Month:**  
   - Identify and display the film with the highest revenue for each month (optionally filtered by a specific year).

## Functions Overview

### `analyze_store_revenue(spark)`
- **Purpose:** Aggregates total revenue per store per year and displays a bar chart.
- **Output:** A PySpark DataFrame with revenue details and a visual bar chart.

### `get_actor_revenue(spark, year_filter=2005)`
- **Purpose:** Retrieves total revenue per actor for the specified year.
- **Output:** A PySpark DataFrame with actor names and their corresponding revenue.

### `get_top_movie_each_month(spark, release_year=None)`
- **Purpose:** Retrieves the film with the highest total revenue for each month (and year). You can optionally filter by a specific release year.
- **Output:** A PySpark DataFrame containing `year`, `month`, `film_name`, and `total_revenue`.

## License

This project is licensed under the MIT License.

## Contact

For any questions or suggestions, please contact [Your Name] at [Your Email].
```

---

Feel free to customize the content (such as the project description, contact information, or any other details) as needed for your project.