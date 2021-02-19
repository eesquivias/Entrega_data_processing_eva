package io.keepcoding.spark.exercise.batch
import io.keepcoding.spark.exercise.streaming.StreamingEntregaImplementacion.spark
import org.apache.spark.sql.functions.{lit, sum, window}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime
import scala.concurrent.Future

object batchJobImpl extends BatchJob {
  override val spark: SparkSession =
   SparkSession
    .builder()
    .master("local[*]")
    .appName("StreamingEntrega")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(storagePath)
      .where($"year"=== filterDate.getYear &&
                $"month" === filterDate.getMonthValue &&
                $"day" === filterDate.getDayOfMonth &&
                $"hour" === filterDate.getHour
      )
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

  //override def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame = ???
  def computeBytesCountByANT(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {

    dataFrame
      .select($"timestamp".cast(TimestampType), $"antenna_id", $"bytes")

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

  override def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame = ???

  override def computePercentStatusByID(dataFrame: DataFrame): DataFrame = ???

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = ???

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = ???

  //def main (args: Array[String]): Unit = run(args)
  def main (args: Array[String]): Unit = {
  val argsTime = "2021-02-19T21:00:00Z"
readFromStorage("C:\\Users\\evaes\\Documents\\GitHub\\Entrega_data_processing_eva\\exerciseEva\\src\\main\\resources\\practica\\data", OffsetDateTime.parse(argsTime))
  .show()
  }
}
