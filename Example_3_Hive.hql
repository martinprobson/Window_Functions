
SELECT emp_no, 
       salary, 
       from_date, 
       to_date, 
       r
FROM   ( SELECT emp_no, 
                salary, 
                from_date, 
                to_date, 
                rank() OVER ( ORDER BY salary DESC) AS r 
         FROM   salaries_t
         WHERE  from_date <= current_date() AND 
                to_date >=  current_date() ) a
WHERE r in (1,2);
