# Window Functions in Hive and Spark

## Introduction
SQL supports two kinds of functions that calculate a single return value. 

- *Built in functions (UDFs)*, such as `substr` or `round`, take 
values from a single row as input and return a single value for every row. 
- *Aggregate functions*, such as `SUM`, `MAX` or `COUNT`, operate on a group of 
rows and return a single value for each group.

There are still a wide range of operations that cannot be expressed using these functions alone. Specifically there was no way to both operate on a
group of rows, while still returning a single value for every input row. 

This limitation makes it hard to perform certain data processing tasks, such 
as calculating a moving average, a cumulative sum, or accessing the values in a row appearing before the current row. 

Window functions are designed to 
fill this gap.

## Setup
A working spark/hive installation is required to execute these examples. 

To load the test data into Hive run the `SetupTestTables.scala` script in *spark-shell*, like: -

```bash
spark-shell --master local[*] -i SetupTestTables.scala
```

## Example 1 - Simple Lead and Lag Window function in Hive
Given the sample table, *foo*: -
```
scala> sql("select * from foo").show
+----------+
|     date1|
+----------+
|2000-01-01|
|2000-01-02|
|2000-01-03|
|2000-01-04|
|2000-01-05|
|2000-01-06|
|2000-01-07|
|2000-01-08|
|2000-01-09|
|2000-01-10|
|2000-01-11|
|2000-01-12|
|2000-01-13|
|2000-01-14|
|2000-01-15|
|2000-01-16|
|2000-01-17|
|2000-01-18|
|2000-01-19|
|2000-01-20|
+----------+
only showing top 20 rows
```
we can use the following Hive HQL statement to produce a result containing previous and next dates (note the null edge cases at each end of the range): -

```
select lag(date1,1) over (order by date1) as previous, 
       date1, 
       lead(date1,1) over (order by date1) as next 
from foo;

Previous        date1           next
NULL            2000-01-01	2000-01-02
2000-01-01	2000-01-02	2000-01-03
2000-01-02	2000-01-03	2000-01-04
2000-01-03	2000-01-04	2000-01-05
2000-01-04	2000-01-05	2000-01-06
2000-01-05	2000-01-06	2000-01-07
2000-01-06	2000-01-07	2000-01-08
2000-01-07	2000-01-08	2000-01-09
2000-01-08	2000-01-09	2000-01-10
2000-01-09	2000-01-10	2000-01-11
...
2000-12-26	2000-12-27	2000-12-28
2000-12-27	2000-12-28	2000-12-29
2000-12-28	2000-12-29	2000-12-30
2000-12-29	2000-12-30	2000-12-31
2000-12-30	2000-12-31	NULL
```
[Example_1_Hive.hql](Example_1_Hive.hql)

## Example 1 - Simple Lead and Lag Window function in Spark
The equivalent Spark code to produce the above is: -
```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Create a windowSpec
val wSpec = Window.orderBy("date1")
val foo = sql("select * from foo")
foo.withColumn("previous",lag(foo("date1"),1).over(wSpec))
   .withColumn("next",lead(foo("date1"),1).over(wSpec))
   .select("previous","date1","next")
   .show

18/03/26 09:50:13 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+----------+----------+----------+
|  previous|     date1|      next|
+----------+----------+----------+
|      null|2000-01-01|2000-01-02|
|2000-01-01|2000-01-02|2000-01-03|
|2000-01-02|2000-01-03|2000-01-04|
|2000-01-03|2000-01-04|2000-01-05|
|2000-01-04|2000-01-05|2000-01-06|
|2000-01-05|2000-01-06|2000-01-07|
|2000-01-06|2000-01-07|2000-01-08|
|2000-01-07|2000-01-08|2000-01-09|
|2000-01-08|2000-01-09|2000-01-10|
|2000-01-09|2000-01-10|2000-01-11|
|2000-01-10|2000-01-11|2000-01-12|
|2000-01-11|2000-01-12|2000-01-13|
|2000-01-12|2000-01-13|2000-01-14|
|2000-01-13|2000-01-14|2000-01-15|
|2000-01-14|2000-01-15|2000-01-16|
|2000-01-15|2000-01-16|2000-01-17|
|2000-01-16|2000-01-17|2000-01-18|
|2000-01-17|2000-01-18|2000-01-19|
|2000-01-18|2000-01-19|2000-01-20|
|2000-01-19|2000-01-20|2000-01-21|
+----------+----------+----------+
only showing top 20 rows
```

[Example_1_Spark.scala](Example_1_Spark.scala)


## Example 2 - Calculate Employee raise percentages - Hive

Given an employee salary tables *salaries_t*: -
```
scala> sql("select * from salaries_t").filter($"emp_no" === 10001).show
+------+------+----------+----------+                                           
|emp_no|salary| from_date|   to_date|
+------+------+----------+----------+
| 10001| 60117|1986-06-26|1987-06-26|
| 10001| 62102|1987-06-26|1988-06-25|
| 10001| 66074|1988-06-25|1989-06-25|
| 10001| 66596|1989-06-25|1990-06-25|
| 10001| 66961|1990-06-25|1991-06-25|
| 10001| 71046|1991-06-25|1992-06-24|
| 10001| 74333|1992-06-24|1993-06-24|
| 10001| 75286|1993-06-24|1994-06-24|
| 10001| 75994|1994-06-24|1995-06-24|
| 10001| 76884|1995-06-24|1996-06-23|
| 10001| 80013|1996-06-23|1997-06-23|
| 10001| 81025|1997-06-23|1998-06-23|
| 10001| 81097|1998-06-23|1999-06-23|
| 10001| 84917|1999-06-23|2000-06-22|
| 10001| 85112|2000-06-22|2001-06-22|
| 10001| 85097|2001-06-22|2002-06-22|
| 10001| 88958|2002-06-22|9999-01-01|
+------+------+----------+----------+
```
Calculate the percentage raise per year.
```
select emp_no, 
       from_date,
       salary,
       prev_salary, 
       round(((1 - prev_salary/salary) * 100),2) as change
from (select emp_no, 
             from_date, 
             salary, 
             lag(salary,1,salary) over (partition by emp_no order by from_date) as prev_salary
      from salaries_t) a 
where emp_no = 10001;

10001	1986-06-26	60117	60117	0.0
10001	1987-06-26	62102	60117	3.2
10001	1988-06-25	66074	62102	6.01
10001	1989-06-25	66596	66074	0.78
10001	1990-06-25	66961	66596	0.55
10001	1991-06-25	71046	66961	5.75
10001	1992-06-24	74333	71046	4.42
10001	1993-06-24	75286	74333	1.27
10001	1994-06-24	75994	75286	0.93
10001	1995-06-24	76884	75994	1.16
10001	1996-06-23	80013	76884	3.91
10001	1997-06-23	81025	80013	1.25
10001	1998-06-23	81097	81025	0.09
10001	1999-06-23	84917	81097	4.5
10001	2000-06-22	85112	84917	0.23
10001	2001-06-22	85097	85112	-0.02
10001	2002-06-22	88958	85097	4.34
Time taken: 39.575 seconds, Fetched: 17 row(s)
```
[Example_2_Hive.hql](Example_2_Hive.hql)


## Example 2 - Calculate Employee raise percentages - Spark

The spark version is a bit more complex, because we have to deal with the *null* at either end of the window in a different manner.

1. Define our window specification. In this case partitioning by *emp_no* and ordering by *from_date*.
2. Then: -
 - Generate a column called *temp_prev* with a lag of one using our window specification. *temp_prev* will be equal to null for the first row.
 - Generate a *prev_salary* column, replace the null with *salary* on the first row and then drop the *temp_prev* column.
3. Calculate the percentage change column, using *prev_salary* and *salary*. Note the use of the *lit* function to generate a literal column equal to one.



```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
// 1.
val wSpec = Window.partitionBy("emp_no").orderBy("from_date")
val sals = sql("select * from salaries_t")
// 2.
val s = sals.filter($"emp_no" === 10001)
            .withColumn("temp_prev",lag($"salary",1).over(wSpec))
            .withColumn("prev_salary",
                        when(col("temp_prev").isNull, $"salary").otherwise($"temp_prev"))
            .drop("temp_prev")
// 3.
s.select($"emp_no",$"from_date",$"salary",$"prev_salary")
 .withColumn("change",round(((lit(1) - $"prev_salary" /$"salary") * 100),2))
 .show

+------+----------+------+-----------+------+
|emp_no| from_date|salary|prev_salary|change|
+------+----------+------+-----------+------+
| 10001|1986-06-26| 60117|      60117|   0.0|
| 10001|1987-06-26| 62102|      60117|   3.2|
| 10001|1988-06-25| 66074|      62102|  6.01|
| 10001|1989-06-25| 66596|      66074|  0.78|
| 10001|1990-06-25| 66961|      66596|  0.55|
| 10001|1991-06-25| 71046|      66961|  5.75|
| 10001|1992-06-24| 74333|      71046|  4.42|
| 10001|1993-06-24| 75286|      74333|  1.27|
| 10001|1994-06-24| 75994|      75286|  0.93|
| 10001|1995-06-24| 76884|      75994|  1.16|
| 10001|1996-06-23| 80013|      76884|  3.91|
| 10001|1997-06-23| 81025|      80013|  1.25|
| 10001|1998-06-23| 81097|      81025|  0.09|
| 10001|1999-06-23| 84917|      81097|   4.5|
| 10001|2000-06-22| 85112|      84917|  0.23|
| 10001|2001-06-22| 85097|      85112| -0.02|
| 10001|2002-06-22| 88958|      85097|  4.34|
+------+----------+------+-----------+------+
```
[Example_2_Spark.scala](Example_2_Spark.scala)


## Example 3 - Who are the first and second most highly paid employees? - Hive

Using the same *salaries_t* table as above: -
```
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


10005	94692	2001-09-09	9999-01-01	1
10001	88958	2002-06-22	9999-01-01	2
```

[Example_3_Hive.hql](Example_3_Hive.hql)


## Example 3 - Who are the first and second most highly paid employees? - Spark

```scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._

val wSpec2 = Window.orderBy($"salary".desc)
val sals = sql("select * from salaries_t")
sals.filter($"from_date" <= current_date && $"to_date" >= current_date).withColumn("r",rank().over(wSpec2)).where("r in (1,2)")

scala> sals.filter($"from_date" <= current_date && $"to_date" >= current_date)
           .withColumn("r",rank().over(wSpec2))
           .where("r in (1,2)")
           .show

18/03/26 16:22:22 WARN WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.
+------+------+----------+----------+---+
|emp_no|salary| from_date|   to_date|  r|
+------+------+----------+----------+---+
| 10005| 94692|2001-09-09|9999-01-01|  1|
| 10001| 88958|2002-06-22|9999-01-01|  2|
+------+------+----------+----------+---+
```

[Example_3_Spark.scala](Example_3_Spark.scala)

Martin Robson - 2018
