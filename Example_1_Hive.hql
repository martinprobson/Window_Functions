
SELECT LAG(date1,1) OVER (ORDER BY date1) AS previous, 
       date1, 
       LEAD(date1,1) OVER (ORDER BY date1) AS next 
FROM foo;
