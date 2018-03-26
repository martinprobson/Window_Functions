
/*
 * Setup Test Data
 *
 * Load the csv file of dates (dates.csv) from the same directory as this script and save as a
 * Parquet table in Hive
*/

// Construct local filename (i.e. not HDFS). 
val sep = java.io.File.separator
var fileName = "file:///" + System.getProperty("user.dir") + sep + "data" + sep + "dates.csv" 

/*
 * Read the file into a DataFrame, with single date column called 'date1' and
 * save to a parquet table called 'foo'.
*/
( spark.read.csv(fileName) 
.withColumnRenamed("_c0","date1") 
.withColumn("date1",to_date($"date1")) 
.write.format("parquet") 
.option("compression","gzip") 
.mode("overwrite").saveAsTable("foo")  )

/*
 * Do the same for the salaries dataset.
 *
*/
import org.apache.spark.sql.types._
fileName = "file:///" + System.getProperty("user.dir") + sep + "data" + sep + "salaries.csv" 
( spark.read.option("header","true")
  .csv(fileName) 
  .select($"emp_no".cast(IntegerType),
          $"salary".cast(IntegerType),
          $"from_date".cast(DateType),
          $"to_date".cast(DateType))
  .write.format("parquet") 
  .option("compression","gzip") 
  .mode("overwrite").saveAsTable("salaries_t")  )

