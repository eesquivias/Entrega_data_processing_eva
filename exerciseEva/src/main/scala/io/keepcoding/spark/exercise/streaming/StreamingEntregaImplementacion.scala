package io.keepcoding.spark.exercise.streaming

import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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

  override def computeBytesCountByAPP(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {

    dataFrame
      .select($"timestamp".cast(TimestampType), $"app", $"bytes")
      .withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "5 minutes").as("timestamp"), $"app".as("id"))
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("app_total_bytes"))
      .withColumn("timestamp", $"timestamp.start")
      .select($"timestamp", $"id", $"value", $"type")
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
  }
  def computeBytesCountByANT(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {

    dataFrame
      .select($"timestamp".cast(TimestampType), $"antenna_id", $"bytes")
      .withWatermark("timestamp", "1 minute")
      .groupBy(window($"timestamp", "5 minutes").as("timestamp"), $"antenna_id".as("id"))
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("antenna_total_bytes"))
      .withColumn("timestamp", $"timestamp.start")
      .select($"timestamp", $"id", $"value", $"type")
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
  }
  def computeBytesCountByUSER(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
    dataFrame
      .select($"timestamp".cast(TimestampType),$"id",$"bytes")
      .withWatermark("timestamp","1 minute")
      .groupBy(window($"timestamp","5 minutes").as("timestamp"),$"id")
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("user_total_bytes"))
      .withColumn("timestamp",$"timestamp.start")
      .select($"timestamp",$"id",$"value",$"type")
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
  }

 def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future{
    dataFrame
      .withColumn("year",year($"timestamp".cast(TimestampType)))
      .withColumn("month", month($"timestamp".cast(TimestampType)))
      .withColumn("day", dayofmonth($"timestamp".cast(TimestampType)))
      .withColumn("hour",hour($"timestamp".cast(TimestampType)))
      .writeStream
      .partitionBy("year","month","day","hour")
      .format("parquet")
      .option("path",s"$storageRootPath\\data")
      .option("checkpointLocation",s"$storageRootPath\\checkpoint")
      .start()
      .awaitTermination()
  }

 def main(args: Array[String]): Unit = {
   val userMetadataDF = readAntennaMetadata("jdbc:postgresql://34.76.48.93:5432/postgres", "user_metadata", "postgres", "keepcoding")
   val antennaStreamingDF = parserJsonData(readFromKafka("34.77.160.106:9092", "devices"))
   val storageFuture = writeToStorage(antennaStreamingDF, "C:\\Users\\evaes\\Documents\\GitHub\\Entrega_data_processing_eva\\exerciseEva\\src\\main\\resources\\practica")
   val antennaMetadataDF = enrichAntennaWithMetadata(antennaStreamingDF,userMetadataDF)
  val aggByAPPDFfut= computeBytesCountByAPP(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "bytes", "postgres", "keepcoding")
   val aggByANTDFfut= computeBytesCountByANT(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "bytes", "postgres", "keepcoding")
   val aggByUSERDFfut= computeBytesCountByUSER(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "bytes", "postgres", "keepcoding")
   Await.result(Future.sequence(Seq(aggByAPPDFfut,aggByANTDFfut,aggByUSERDFfut,storageFuture)), Duration.Inf)

   // Await.result(storageFuture, Duration.Inf)
 }


}
