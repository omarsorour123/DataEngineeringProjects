from utils.Connection import create_connection
import pandas as pd
import matplotlib.pyplot as plt

# Function to fetch number of sales per customer
def fetch_sales_per_customer():
    engine = create_connection()

    query = """
    SELECT 
        CustomerID, 
        COUNT(InvoiceNo) AS num_sales
    FROM 
        retails
    GROUP BY 
        CustomerID
    HAVING 
        CustomerID IS NOT NULL    
    ORDER BY 
        num_sales DESC
    LIMIT 
        5;
    """

    # Fetching data and returning it as a DataFrame
    df = pd.read_sql(query, engine)
    return df

# Function to fetch total sales by year and month
def fetch_sales_by_year_month():
    engine = create_connection()

    query = """
    SELECT 
        EXTRACT(YEAR FROM InvoiceDate) AS year, 
        EXTRACT(MONTH FROM InvoiceDate) AS month, 
        ROUND(SUM(Quantity * UnitPrice)::NUMERIC , 0) AS total_price
    FROM 
        retails
    GROUP BY
        year, month 
    ORDER BY
       year, month;
    """

    # Fetching data and returning it as a DataFrame
    df = pd.read_sql(query, engine)
    return df

# Function to fetch best 10 sold products by Quantity
def fetch_best_10_sold_products():
    engine = create_connection()

    query = """
    SELECT 
        stockcode,
        SUM(Quantity) AS quantity
    FROM
        retails
    GROUP BY
        stockcode    
    ORDER BY
        quantity DESC
    LIMIT
        10;
    """

    # Fetching data and returning it as a DataFrame
    df = pd.read_sql(query, engine)
    return df

# Function to fetch worst 10 sold products by Quantity
def fetch_worst_10_sold_products():
    engine = create_connection()

    query = """
    SELECT 
        stockcode,
        SUM(Quantity) AS quantity
    FROM
        retails
    GROUP BY
        stockcode    
    ORDER BY
        quantity ASC
    LIMIT
        10;
    """

    # Fetching data and returning it as a DataFrame
    df = pd.read_sql(query, engine)
    return df

# Function to fetch best 5 sales by country
def fetch_best_sales_by_country():
    engine = create_connection()

    query = """
    SELECT 
        Country, 
        ROUND(SUM(Quantity * UnitPrice)::NUMERIC, 0) AS total_sales
    FROM 
        retails
    GROUP BY
        Country
    ORDER BY
        total_sales DESC
    LIMIT 
        5;
    """

    # Fetching data and returning it as a DataFrame
    df = pd.read_sql(query, engine)
    return df

# Function to plot the number of sales per customer
def plot_sales_per_customer(df):
    plt.figure(figsize=(10, 6))
    plt.bar(df['customerid'].astype(str), df['num_sales'], color='green')
    plt.title('Top 5 Customers by Number of Sales', fontsize=16)
    plt.xlabel('CustomerID', fontsize=12)
    plt.ylabel('Number of Sales', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Function to plot total sales by year and month
def plot_sales_by_year_month(df):
    df['date'] = pd.to_datetime(df[['year', 'month']].assign(day=1))
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['total_price'], marker='o', linestyle='-', color='b', label='Total Sales')
    plt.title('Monthly Sales Trend', fontsize=16)
    plt.xlabel('Date', fontsize=12)
    plt.ylabel('Total Sales', fontsize=12)
    plt.xticks(rotation=45)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.show()

# Function to plot best 10 sold products by quantity
def plot_best_sold_products(df):
    plt.figure(figsize=(10, 6))
    plt.bar(df['stockcode'].astype(str), df['quantity'], color='blue')
    plt.title('Top 10 Best Sold Products by Quantity', fontsize=16)
    plt.xlabel('StockCode', fontsize=12)
    plt.ylabel('Quantity Sold', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Function to plot worst 10 sold products by quantity
def plot_worst_sold_products(df):
    plt.figure(figsize=(10, 6))
    plt.bar(df['stockcode'].astype(str), df['quantity'], color='red')
    plt.title('Top 10 Worst Sold Products by Quantity', fontsize=16)
    plt.xlabel('StockCode', fontsize=12)
    plt.ylabel('Quantity Sold', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Function to plot best 5 sales by country
def plot_best_sales_by_country(df):
    plt.figure(figsize=(10, 6))
    plt.bar(df['Country'], df['total_sales'], color='purple')
    plt.title('Top 5 Countries by Sales', fontsize=16)
    plt.xlabel('Country', fontsize=12)
    plt.ylabel('Total Sales', fontsize=12)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


# Main block to run the queries and plot the results

if __name__ == "__main__":
    # Fetching data
    sales_per_customer = fetch_sales_per_customer()
    sales_by_year_month = fetch_sales_by_year_month()
    best_10_sold_products = fetch_best_10_sold_products()
    worst_10_sold_products = fetch_worst_10_sold_products()
    best_sales_by_country = fetch_best_sales_by_country()

    # Plotting the data
    plot_sales_per_customer(sales_per_customer)
    plot_sales_by_year_month(sales_by_year_month)
    plot_best_sold_products(best_10_sold_products)
    plot_worst_sold_products(worst_10_sold_products)
    plot_best_sales_by_country(best_sales_by_country)


