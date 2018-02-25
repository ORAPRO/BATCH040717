package com.laboros.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.Subscribe
import org.apache.spark.streaming.kafka010.PreferConsistent
import org.apache.spark.streaming.kafka010.LocationStrategies
import org.apache.spark.streaming.kafka010.ConsumerStrategies
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.spark.TaskContext

//cd /home/edureka/SAIWS/BATCHES
//spark-submit --verbose --master local --deploy-mode client --conf spark.driver.extraClassPath=/home/edureka/SAIWS/BATCHES/mysql-connector-java-5.1.45-bin.jar:/usr/lib/kafka_2.12-0.11.0.0/libs/kafka-clients-0.11.0.0.jar --class com.laboros.spark.sql.SparkStreamingConsumer sparksqldemo_2.11-1.0.jar kafka_topic --packages org.apache.spark:spark-streaming-kafka-0-10:2.0.0


object SparkStreamingConsumer {
  def main(args: Array[String]): Unit = {

    //    val spark = SparkSession.builder().appName("Spark Producer").getOrCreate();
    //
    //    import spark.implicits._;
    //    import spark.sql;

    val topic = args(0);

    println("Current Topic Name " + topic);

    val DefaultZookeeperConnection = "127.0.0.1:2181"
    val DefaultKafkaConnection = "127.0.0.1:9092"

    val props: Map[String, Object] = Map(
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val numberOfReceivers = 1

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topics = Array(args(0));
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, props))

    //      stream.map(record => (record.key, record.value)).flatMap(flatMapFunc);

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }
    
    
    stream.foreachRDD { rdd =>
      rdd.foreach(CR=> println(CR.key()+","+CR.value()+","+CR.value()));
    }
    ssc.start();
    ssc.awaitTermination();

  }
}