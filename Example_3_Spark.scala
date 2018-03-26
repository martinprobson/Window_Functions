
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._

val wSpec2 = Window.orderBy($"salary".desc)
val sals = sql("select * from salaries_t")
sals.filter($"from_date" <= current_date && $"to_date" >= current_date).withColumn("r",rank().over(wSpec2)).where("r in (1,2)").show
