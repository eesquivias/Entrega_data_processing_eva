package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object StreamingEntregaImplementacion extends StreamingJob {
  override val spark: SparkSession =
    SparkSession
    .builder()
      .master("local[*]")
      .appName("StreamingEntrega")
      .getOrCreate()

  import spark.implicits._

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame =
    spark
      .readStream
      .format ("kafka")
      .option("kafka.bootstrap.servers", kafkaServer) //"34.77.160.106:9092" en vez de kafkaServer
      //.option("kafka.bootstrap.servers", "34.77.160.106:9092")
      .option("subscribe", topic) //"devices" en vez de topic
      //.option("subscribe", "devices")
      .load()

  override def parserJsonData(dataFrame: DataFrame): DataFrame = {
   val schema= ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]
    dataFrame
      .select(from_json($"value".cast(StringType),schema).as("json"))
      .select("json.*")
  }

  override def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user",user)
      .option("password",password)
      .load()
  }

  override def enrichAntennaWithMetadata(antennaStreamingDF: DataFrame, userMetadataDF: DataFrame): DataFrame = {
    antennaStreamingDF
      .join(userMetadataDF,
        antennaStreamingDF("id")===userMetadataDF("id")
      ).drop(userMetadataDF("id"))
  }

  override def computeBytesCountByID(dataFrame: DataFrame): DataFrame = {
    //dataFrame
    val dataFrameApp = dataFrame
      .select($"timestamp",$"app",$"bytes")
      .groupBy($"timestamp",$"app".as("id"))
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("app_total_bytes"))

    val dataFrameAnt = dataFrame
      .select($"timestamp",$"antenna_id",$"bytes")
      .groupBy($"timestamp",$"antenna_id".as("id"))
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("antenna_total_bytes"))

    val dataFrameUser = dataFrame
      .select($"timestamp",$"id",$"bytes")
      .groupBy($"timestamp",$"id")
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("user_total_bytes"))

    dataFrameApp.unionByName(dataFrameAnt).unionByName(dataFrameUser)

    /*  //.filter($"app"=== lit("FACEBOOK"))
      .select($"timestamp", $"app", $"bytes")
      .withWatermark("timestamp", "1 minute")
      .groupBy($"app",window($"timestamp","5 minutes"))
      .agg(
        collect_list($"app").as("AppTotalBytes"),
        collect_list($"antenna_id").as("AntennaTotalBytes"),
        collect_list($"id").as("UserTotalBytes")
      )
      .withColumn("id",concat($"app".as("AppTotalBytes"),$"antenna_id".as("AntennaTotalBytes"),$"id".as("UserTotalBytes")))
        //sum($"bytes").as("total_bytes_app"),
        //sum($"bytes").as("total_bytes_ant"),
        //sum($"bytes").as("total_bytes_userid"),
      .select($"timestamp",$"type",$"bytes")*/
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = ??? /*Future {
    dataFrame
      .writeStream
      .foreachBatch { (df: DataFrame, id: Long) =>
        df
          .write
          .mode(SaveMode.Append)
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", jdbcURI)
          .option("dbtable", jdbcTable)
          .option("user", user)
          .option("password", password)
          .save()
      }.start()
      .awaitTermination()
  }*/

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = ??? /* Future{
    dataFrame
      .withColumn("year",year($"timestamp"))
      .withColumn("month", month($"timestamp"))
      .withColumn("day", dayofmonth($"timestamp"))
      .withColumn("hour",hour($"timestamp"))
      .writeStream
      .partitionBy("year","month","day","hour")
      .format("parquet")
      .option("path",s"$storageRootPath/data")
      .option("checkpointLocation",s"$storageRootPath/checkpoint")
      .start()
      .awaitTermination()
  }*/

  // def main(args: Array[String]): Unit = run(args)

 def main(args: Array[String]): Unit = {
   val userMetadataDF = readAntennaMetadata("jdbc:postgresql://34.76.48.93:5432/postgres", "user_metadata", "postgres", "keepcoding")
   val antennaStreamingDF = parserJsonData(readFromKafka("34.77.160.106:9092", "devices"))

   computeBytesCountByID(enrichAntennaWithMetadata(antennaStreamingDF,userMetadataDF))

  // parserJsonData(readFromKafka("34.77.160.106:9092", "devices"))
   //readFromKafka("34.77.160.106:9092", "devices")
     .writeStream
     .format("console")
     .start()
     .awaitTermination()
 }

/*
    val writeToStorageFut
    computeBytesCountByID(enrichAntennaWithMetadata(antennaStreamingDF,userMetadataDF))
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }*/
}
