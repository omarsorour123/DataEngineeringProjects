# Retail Data Analysis Project

This project focuses on analyzing retail sales data using SQL and Python. The primary goal is to practice SQL queries, data ingestion, and data visualization as part of your Data Engineering learning plan. The analysis involves processing and visualizing retail data to derive insights such as sales trends, best-selling products, and customer behavior.

## Technologies Used

- **Programming Languages:**
  - Python
- **Python Libraries:**
  - Pandas
  - Matplotlib
  - SQLAlchemy
- **Database:**
  - PostgreSQL
- **Tools:**
  - Jupyter Notebook
  - PostgreSQL Client
  - Python Scripts

## Project Structure

```bash
Retail_Data_Analysis_Project/
├── data/
│   └── Online_Retail.xlsx
├── scripts/
│   ├── data_ingestion.py
│   └── main.py
├── requirements.txt
├── README.md
└── retail_env/
```

## Setup Instructions

### 1. Download the Data

Download the dataset from the [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/352/online+retail) and place it in the `data/` directory.

- **Dataset:** `Online_Retail.xlsx`

### 2. Install Dependencies

#### a. Create a Python Virtual Environment

```bash
python -m venv retail_env
```

#### b. Activate the Virtual Environment

- **On Windows:**

  ```bash
  retail_env\Scripts\activate
  ```

- **On macOS/Linux:**

  ```bash
  source retail_env/bin/activate
  ```

#### c. Install Required Python Libraries

Ensure you have a `requirements.txt` file with the necessary dependencies listed. Then run:

```bash
pip install -r requirements.txt
```

### 3. Ingest Data into the Database

Run the `data_ingestion.py` script to ingest data from the Excel file into the PostgreSQL database.

```bash
python data_ingestion.py
```

### 4. Run the Analysis and Visualizations

Once the data is ingested, execute the `main.py` script to analyze the data and generate visualizations.

```bash
python main.py
```

## Additional Information

- **Jupyter Notebook:** You can also use Jupyter Notebook for interactive data analysis and visualization.

  ```bash
  jupyter notebook
  ```

- **PostgreSQL Client:** Ensure PostgreSQL is installed and properly configured on your machine to handle data storage and queries.

## Acknowledgements

- [UCI Machine Learning Repository](https://archive.ics.uci.edu/dataset/352/online+retail) for providing the dataset.

# Happy Analyzing!

---
