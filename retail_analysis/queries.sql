-- Creating Table to handle the data --
-- the goal is not putting a constrains i will apply this in data modeling projects--

DROP TABLE retails;

CREATE TABLE retails
(
InvoiceNo INT,
StockCode INT,
Description	VARCHAR(100),
Quantity	INT,
InvoiceDate	DATE,
UnitPrice	REAL,
CustomerID	INT,
Country	VARCHAR(100)
);

SELECT * FROM retails LIMIT 5;
