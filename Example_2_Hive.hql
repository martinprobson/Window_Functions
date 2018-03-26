
SELECT emp_no, 
       from_date,
       salary,
       prev_salary, 
       ROUND(((1 - prev_salary/salary) * 100),2) AS change
FROM  (SELECT emp_no, 
              from_date, 
              salary, 
              LAG(salary,1,salary) OVER (PARTITION BY emp_no ORDER BY from_date) AS prev_salary
       FROM salaries_t) a 
WHERE emp_no = 10001;
