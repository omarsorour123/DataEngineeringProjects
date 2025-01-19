-- Number of sales per customer --
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
-- total sales by year and month --
SELECT 
    EXTRACT(YEAR FROM InvoiceDate) AS year, EXTRACT (MONTH FROM InvoiceDate) AS month, ROUND(SUM(Quantity * UnitPrice)::NUMERIC , 0) AS total_price
FROM 
    retails
GROUP BY
    year,month 
ORDER BY
   year, month;


-- best 10 sold products by Quantity --
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

-- worst 10 sold products by Quantity --
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


-- best 5 sales By country --
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


