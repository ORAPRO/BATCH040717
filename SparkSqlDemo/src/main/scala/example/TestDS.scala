package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object TestDS {
  case class Person(name: String, age: Long)
  
  val p = new Person("a",20);
  val p1 = new Person("a",20);

  println(p == p1);
  
  def main(args: Array[String]): Unit = {
    
  val sparkConf = new SparkConf().setAppName("Test DataFrames"); //Remaining will use properties file

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate();

  import spark.implicits._; // Convert RDD to DataFrames

  val caseClassDS = Seq(Person("Andy", 32)).toDS()

  caseClassDS.show()

  val primitiveDS = Seq(1, 2, 3).toDS()
  primitiveDS.map(_ + 1).collect()

  val path = args(0);
  val peopleDS = spark.read.json(path).as[Person]
  peopleDS.show()
    
  }
}