package example

import org.apache.spark.sql.SparkSession

object TestDataFrame {
  
  def main(args: Array[String]): Unit = {
    
    
    //Create SparkSession 
    val spark:SparkSession = SparkSession
                            .builder()
                            .appName("Test Data Frame")
                            .getOrCreate();
    //Import Implicits
    
    import spark.implicits._;
    
    val df = spark.read.json(args(0));
    
    df.printSchema();
    
    df.show();
    
    df.select("name").show();
    
    df.select($"name", $"age" + 1).show()
    
    df.filter($"age" > 21).show()
    
    df.groupBy("age").count().show()
    
//    df.createTempView("people");
//    
//    spark.sql("select name, age from people")
    
    
  }
}