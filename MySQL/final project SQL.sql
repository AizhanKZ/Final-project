create database final_project;
SET SQL_SAFE_UPDATES = 0;
UPDATE customers SET Gender = NULL WHERE Gender='';
UPDATE customers SET Age = NULL WHERE Age='';
ALTER TABLE customers MODIFY Age INT NULL;

CREATE TABLE Transactions
(date_new DATE,
Id_check INT,
ID_client INT,
Count_products DECIMAL(10,3),
Sum_payment DECIMAL(10,2));

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\transactions_final.csv"
INTO TABLE Transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
#1
WITH monthly AS (
    SELECT
        ID_client,
        DATE_FORMAT(date_new, '%Y-%m-01') AS month,
        COUNT(DISTINCT Id_check) AS operations_cnt,
        SUM(Sum_payment) AS month_sum,
        AVG(Sum_payment) AS avg_check_month
    FROM transactions
    WHERE date_new BETWEEN '2015-06-01' AND '2016-05-31'
    GROUP BY ID_client, DATE_FORMAT(date_new, '%Y-%m-01')
), 
full_clients AS (
    SELECT ID_client
    FROM monthly
    GROUP BY ID_client
    HAVING COUNT(DISTINCT month) = 12
)
SELECT
    m.ID_client,
    m.month,
    m.operations_cnt,
    m.month_sum,
    m.avg_check_month,
    SUM(m.month_sum) OVER (PARTITION BY m.ID_client)
        / SUM(m.operations_cnt) OVER (PARTITION BY m.ID_client) AS avg_check_period,
     AVG(m.month_sum) OVER (PARTITION BY m.ID_client) AS avg_month_sum,
     SUM(m.operations_cnt) OVER (PARTITION BY m.ID_client) AS total_operations
FROM monthly m
JOIN full_clients f
    ON m.ID_client = f.ID_client
ORDER BY m.ID_client, m.month;
#2
SELECT month(date_new) AS month, AVG(sum_payment) AS AVG
FROM Transactions
GROUP BY month(date_new)
ORDER BY month(date_new);

SELECT month(date_new) AS month,
AVG(Id_check) AS AVG
FROM Transactions
GROUP BY month(date_new)
ORDER BY month(date_new);

WITH monthly_ops AS (
    SELECT 
        MONTH(date_new) AS month,
        COUNT(DISTINCT Id_check) AS operations_cnt
    FROM Transactions
    GROUP BY MONTH(date_new)
)
SELECT 
    AVG(operations_cnt) AS avg_operations_per_month
FROM monthly_ops;

    SELECT 
        MONTH(date_new) AS month,
        COUNT(DISTINCT ID_client) AS client_cnt
    FROM Transactions
    GROUP BY MONTH(date_new);

SELECT 
    MONTH(date_new) AS month,
    COUNT(DISTINCT Id_check) AS operations_cnt,
    SUM(sum_payment) AS month_sum,
   COUNT(DISTINCT Id_check) * 1.0 
        / SUM(COUNT(DISTINCT Id_check)) OVER () AS share_operations,
   SUM(sum_payment) * 1.0 
        / SUM(SUM(sum_payment)) OVER () AS share_amount
FROM Transactions
GROUP BY MONTH(date_new)
ORDER BY month;

SELECT MONTH(t.date_new) AS month,
c.Gender,
sum(t.sum_payment) AS payment,
ROUND(SUM(t.sum_payment) * 100.0 
        / SUM(SUM(t.sum_payment)) OVER (PARTITION BY MONTH(t.date_new)),2customers) AS share
FROM customers AS c
JOIN transactions as t
ON c.Id_client=t.ID_client
GROUP BY  MONTH(t.date_new),c.Gender
ORDER BY MONTH(t.date_new),c.Gender DESC;
#3
WITH base AS (
    SELECT 
        t.Id_check,
        t.sum_payment,
        QUARTER(t.date_new) AS quarter,
        CASE 
            WHEN c.Age IS NULL THEN 'No Age'
            ELSE CONCAT(FLOOR(c.Age/10)*10, '-', FLOOR(c.Age/10)*10 + 9)
        END AS age_group
        FROM transactions t
    JOIN customers c 
        ON t.ID_client = c.Id_client
),
agg AS (
    SELECT 
        age_group,
        quarter,
        SUM(sum_payment) AS total_sum,
        COUNT(DISTINCT Id_check) AS operations_cnt
    FROM base
    GROUP BY age_group, quarter
)
SELECT 
    age_group,
    quarter,
    total_sum,
    operations_cnt,
       ROUND(total_sum * 1.0 / operations_cnt,2) AS avg_check,
       ROUND(total_sum * 100.0 
        / SUM(total_sum) OVER (PARTITION BY quarter),2) AS share_sum,
        ROUND(operations_cnt * 100.0 
        / SUM(operations_cnt) OVER (PARTITION BY quarter),2) AS share_ops
FROM agg
ORDER BY quarter, age_group;