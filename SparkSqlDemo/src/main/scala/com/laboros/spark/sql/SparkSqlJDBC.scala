package com.laboros.spark.sql

import org.apache.spark.sql.SparkSession
import java.util.Properties

//spark-submit --verbose --master local --deploy-mode client --conf spark.driver.extraClassPath=/home/edureka/SAIWS/BATCHES/mysql-connector-java-5.1.45-bin.jar --class com.laboros.spark.sql.SparkSqlJDBC sparksqldemo_2.11-1.0.jar
object SparkSqlJDBC {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSqlJDBC").getOrCreate();

    import spark.implicits._;
    import spark.sql;

    val jdbcUsername = "root"
    val jdbcPassword = "Edurekasql_123"
    val jdbcHostname = "localhost"
    val jdbcPort = 3306
    val jdbcDatabase = "mydb"
    val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}?user=${jdbcUsername}&password=${jdbcPassword}"

    val conProps = new Properties()
    conProps.put("user", "root")
    conProps.put("password", "Edurekasql_123")
    conProps.put("driver", "com.mysql.jdbc.Driver")

    val edf = spark.read.jdbc(jdbcUrl, "elections", conProps)
    edf.printSchema();
    edf.select("fips", "Households").limit(10).show()

  }
}