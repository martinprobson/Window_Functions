
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
// 1.
val wSpec = Window.partitionBy("emp_no").orderBy("from_date")
val sals = sql("select * from salaries_t")
// 2.
val s = ( sals.filter($"emp_no" === 10001)
              .withColumn("temp_prev",lag($"salary",1).over(wSpec))
              .withColumn("prev_salary",when(col("temp_prev").isNull, $"salary").otherwise($"temp_prev")).drop("temp_prev") )
// 3.
( s.select($"emp_no",$"from_date",$"salary",$"prev_salary")
 .withColumn("change",round(((lit(1) - $"prev_salary" /$"salary") * 100),2))
 .show )
