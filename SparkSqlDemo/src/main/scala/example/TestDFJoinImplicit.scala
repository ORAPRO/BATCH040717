package example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType

object TestDFJoinImplicit {

//  case class customer(cid: Long, fname: String, lname: String, age: Int, desg: String);

//  case class txn(txnid: Long, dt: String, ctid: Long, amount: Float, prod: String, cat: String, city: String, state: String, ct: String);

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Test DataFrames"); //Remaining will use properties file

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate();

    import spark.implicits._; // Convert RDD to DataFrames
    
    val customerSchema = "cid,fname,lname,age,desg";
    val txnSchema = "txnid,dt,ctid,amount,prod,cat,city,state,ct";
    
    val custFieldsA = customerSchema.split(",");
    
//    StructType(Seq(StructField(custFieldsA(0),LongType),
//        StructField(custFieldsA(1),StringType),
//        StructField(custFieldsA(2),StringType),
//        StructField(custFieldsA(3),IntegerType),
//        StructField(custFieldsA(4),StringType)
//    ));
    
    val custFields=customerSchema.split(",").map(e=>StructField(e,StringType));
    
    val custStruct = StructType(custFields);
    
    val txnFields = txnSchema.split(",").map(e=>StructField(e,StringType));
    val txnStruct = StructType(txnFields);

    val inputCustRDD = spark.sparkContext.textFile(args(0), 10);
    val inputTxnRDD = spark.sparkContext.textFile(args(1), 10);
    
    val custRows=inputCustRDD.map(_.split(",")).filter(_.length==5).map(cols=>Row(
        cols(0).trim(),
        cols(1).trim(),
        cols(2).trim(),
        cols(3).trim(),
        cols(4).trim()
        )
        );
    val txnRows = inputTxnRDD.map(_.split(",")).filter(_.length==9).map(cols=>Row(
        cols(0).trim(),
        cols(1).trim(),
        cols(2).trim(),
        cols(3).trim(),
        cols(4).trim(),
        cols(5).trim(),
        cols(6).trim(),
        cols(7).trim(),
        cols(8).trim())
    );
    
    val custDS=spark.createDataFrame(custRows, custStruct);
    val txnDS = spark.createDataFrame(txnRows,txnStruct);
    
    custDS.createTempView("customer");
    txnDS.createTempView("transaction");
    
    spark.sql("select fname,count(*),sum(cast(amount as float)) from customer c JOIN transaction t on c.cid = t.ctid group by fname").show();
  }
}