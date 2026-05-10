with Transactions as 
(
    SELECT * FROM
    {{source('raw_sync_data','financial_data')}}
)

SELECT  category,
        SUM(amount) as total_amount
FROM Transactions
GROUP BY category        