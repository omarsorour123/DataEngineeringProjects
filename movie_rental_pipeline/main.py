from pyspark.sql import SparkSession
from utils.config import get_config
from utils.common_pyspark import read_table_database
from pyspark.sql.functions import sum, year, coalesce, lit, col, count
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Load configuration and initialize Spark session
conf = get_config()
spark = SparkSession.builder.appName('testing') \
     .config('spark.jars', conf['pyspark']['JDBC_DRIVER_PATH']).getOrCreate()

def analyze_store_revenue(spark):
    """
    Reads data from a database, aggregates total revenue per store per year, 
    and generates a bar chart comparing stores' revenues per year.
    """
    # Read data from database
    payment = read_table_database(spark, 'payment')
    staff = read_table_database(spark, 'staff')
    store = read_table_database(spark, 'store')

    # Perform joins and select relevant columns
    df = (payment
          .join(staff, 'staff_id', 'inner')
          .join(store, 'store_id', 'inner')
          .select(store.store_id, year(payment.payment_date).alias('year'), payment.amount))

    # Aggregate total revenue per store per year
    results_df = (df.groupBy("store_id", "year")
                    .agg(sum('amount').alias('total_money'))
                    .orderBy("year", "store_id"))

    # Convert PySpark DataFrame to Pandas for plotting
    pdf = results_df.toPandas()

    # Ensure 'year' and 'total_money' are numeric
    pdf["year"] = pd.to_numeric(pdf["year"], errors="coerce")
    pdf["total_money"] = pd.to_numeric(pdf["total_money"], errors="coerce")

    # Pivot DataFrame for bar chart (Make sure values are numeric)
    pivot_df = pdf.pivot(index="year", columns="store_id", values="total_money").fillna(0)
    pivot_df = pivot_df.apply(pd.to_numeric)

    # Plot bar chart
    pivot_df.plot(kind="bar", figsize=(10, 6))
    plt.xlabel("Year")
    plt.ylabel("Total Revenue")
    plt.title("Store Revenue Comparison Per Year")
    plt.legend(title="Store ID")
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.show()

    return results_df  # Return DataFrame for further analysis if needed

def get_actor_revenue(spark, year_filter=2005):
    """
    Retrieves total revenue per actor for a given year from the movie rental database.
    
    :param spark: SparkSession object
    :param year_filter: Year to filter the payment data (default: 2005)
    :return: DataFrame with actor names and their total revenue
    """
    # Read tables from the database
    payment = read_table_database(spark, 'payment')
    actor = read_table_database(spark, 'actor')
    film_actor = read_table_database(spark, 'film_actor')
    film = read_table_database(spark, 'film')
    inventory = read_table_database(spark, 'inventory')
    rental = read_table_database(spark, 'rental')

    # Perform joins and filter
    df = (
        actor
        .join(film_actor, 'actor_id', 'left')
        .join(film, 'film_id', 'left')
        .join(inventory, 'film_id', 'left')
        .join(rental, 'inventory_id', 'left')
        .join(payment, 'rental_id', 'left')
        .filter(year(col("payment_date")) == year_filter)  # Apply year filter
        .select("first_name", "last_name", "amount")
    )

    # Aggregate and order results
    results = (
        df.groupBy("first_name", "last_name")
        .agg(coalesce(sum("amount"), lit(0)).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
    )
    
    return results

def get_top_movie_each_month(spark, release_year=None):
    """
    Retrieves the film with the highest total revenue for each month (and year).
    
    :param spark: SparkSession object
    :param release_year: Optionally filter by a specific year (default: None, which means all years)
    :return: DataFrame with columns: year, month, film_name, total_revenue
    """
    # Read tables from the database
    payment = read_table_database(spark, 'payment')
    rental = read_table_database(spark, 'rental')
    inventory = read_table_database(spark, 'inventory')
    film = read_table_database(spark, 'film')
    
    # Perform joins
    df = (
        payment
        .join(rental, "rental_id", "inner")
        .join(inventory, "inventory_id", "inner")
        .join(film, "film_id", "inner")
    )
    
    # Extract year and month from payment_date and rename film title as film_name
    df = df.withColumn("year", F.year(F.col("payment_date"))) \
           .withColumn("month", F.month(F.col("payment_date"))) \
           .withColumn("film_name", F.col("title"))
    
    # Optional: Filter by a specific year if provided
    if release_year is not None:
        df = df.filter(F.col("year") == release_year)
    
    # Aggregate total revenue per film per (year, month)
    agg_df = df.groupBy("year", "month", "film_name") \
               .agg(F.sum("amount").alias("total_revenue"))
    
    # Define a window partitioned by year and month, ordering by total_revenue descending
    windowSpec = Window.partitionBy("year", "month").orderBy(F.desc("total_revenue"))
    
    # Rank films within each (year, month) group
    ranked_df = agg_df.withColumn("rev_rank", F.rank().over(windowSpec))
    
    # Filter for the top film (rank = 1) in each (year, month) and order the results
    top_movies_df = ranked_df.filter(F.col("rev_rank") == 1) \
                             .orderBy("year", "month")
    
    return top_movies_df.select("year", "month", "film_name", "total_revenue")

# Execute all functions when running the file
if __name__ == "__main__":
    # 1. Analyze store revenue and display the bar chart
    store_revenue_df = analyze_store_revenue(spark)
    print("Store Revenue DataFrame:")
    store_revenue_df.show(10)
    
    # 2. Get actor revenue for year 2005 and show results
    actor_revenue_df = get_actor_revenue(spark, year_filter=2005)
    print("Actor Revenue DataFrame for 2005:")
    actor_revenue_df.show(10)
    
    # 3. Get top movie each month (across all years)
    top_movie_df = get_top_movie_each_month(spark)
    print("Top Movie Each Month DataFrame:")
    top_movie_df.show(10)
