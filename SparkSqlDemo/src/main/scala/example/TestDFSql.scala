package example

import org.apache.spark.sql.SparkSession

object TestDFSql {
  
  def main(args: Array[String]): Unit = {
    
    
    //Create SparkSession 
    val spark:SparkSession = SparkSession
                            .builder()
                            .appName("Test Data Frame")
                            .getOrCreate();
    //Import Implicits
    
    import spark.implicits._;
    
    val df = spark.read.json(args(0));
    
    
    df.createTempView("people");
   spark.sql("select name, age from people").show();

   spark.sql ("select age,count(*) from people group by age").show();
   
   df.createGlobalTempView("people")
   
   spark.newSession.sql("select name, age from global_temp.people").show();
   
   
    
    
  }
}