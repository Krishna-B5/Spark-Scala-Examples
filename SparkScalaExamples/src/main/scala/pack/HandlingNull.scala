package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object HandlingNull {
  
  def main(args:Array[String]){
	  println("====== Started ======")
    val spark = SparkSession.builder().master("local[*]").appName("SparkscalaByExamples").getOrCreate()
    import spark.implicits._
    
    spark.sparkContext.setLogLevel("ERROR")

    val data = spark.read.format("CSV")
               .option("header", "true")
               .option("inferschema", "true")
               .load("file:///C:/data/spark_scala_examples/small_zipcode.csv")
               
    data.show()
    data.printSchema()
    
    println("==== Only the Integer column null values will be replaced with 0 ===")
    
    data.na.fill(0).show()
    
    println("==== Only the String column null values will be replaced with give values ===")
    
    data.na.fill("Unknown").show()
    
    println("==== This will change only the particular column ===")
    
    data.na.fill(5,Array("population"))
        .na.fill("unknown",Array("city"))
        .na.fill("",Array("type"))
        .na.fill(0,Array("zipcode"))
        .show(false)

	  println("====== Done =======")
  }
}