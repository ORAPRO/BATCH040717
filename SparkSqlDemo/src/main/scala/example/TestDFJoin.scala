package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object TestDFJoin {

  case class customer(cid: Long, fname: String, lname: String, age: Int, desg: String);

  case class txn(txnid: Long, dt: String, ctid: Long, amount: Float, prod: String, cat: String, city: String, state: String, ct: String);

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Test DataFrames"); //Remaining will use properties file

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate();

    import spark.implicits._; // Convert RDD to DataFrames

    val inputCustRDD = spark.sparkContext.textFile(args(0), 10);
    val inputTxnRDD = spark.sparkContext.textFile(args(1), 10);
    
    val custDS=inputCustRDD.map(_.split(",")).filter(_.length==5).map(cols=>customer(
        cols(0).trim().toLong,
        cols(1).trim(),
        cols(2).trim(),
        cols(3).trim().toInt,
        cols(4).trim()
        )
        ).toDS();
    
    val txnDS = inputTxnRDD.map(_.split(",")).filter(_.length==9).map(cols=>txn(
        cols(0).trim().toLong,
        cols(1).trim(),
        cols(2).trim().toLong,
        cols(3).trim().toFloat,
        cols(4).trim(),
        cols(5).trim(),
        cols(6).trim(),
        cols(7).trim(),
        cols(8).trim())
    ).toDS();
    
    custDS.createTempView("customer");
    txnDS.createTempView("transaction");
    
    spark.sql("select fname,count(*),sum(amount) from customer c JOIN transaction t on c.cid = t.ctid group by fname").show();
  }
}