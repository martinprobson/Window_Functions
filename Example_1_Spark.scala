
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
// Create a windowSpec
val wSpec = Window.orderBy("date1")
val foo = sql("select * from foo")
foo.withColumn("previous",lag(foo("date1"),1).over(wSpec)).withColumn("next",lead(foo("date1"),1).over(wSpec)).select("previous","date1","next").show
