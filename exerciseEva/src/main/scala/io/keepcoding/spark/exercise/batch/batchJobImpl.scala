package io.keepcoding.spark.exercise.batch
import org.apache.spark.sql.functions.{lit, sum, window}
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

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

   def computeBytesCountByANT(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .select($"timestamp".cast(TimestampType), $"antenna_id", $"bytes")
      .groupBy(window($"timestamp", "1 hour").as("timestamp"), $"antenna_id".as("id"))
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("antenna_total_bytes"))
      .withColumn("timestamp", $"timestamp.start")
      .select($"timestamp", $"id", $"value", $"type")
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
      }
  override def computeBytesCountByAPP(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .select($"timestamp".cast(TimestampType), $"app", $"bytes")
      .groupBy(window($"timestamp", "1 hour").as("timestamp"), $"app".as("id"))
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("app_total_bytes"))
      .withColumn("timestamp", $"timestamp.start")
      .select($"timestamp", $"id", $"value", $"type")
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }
  def computeBytesCountByUSER(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String):Unit = {
    dataFrame
      .select($"timestamp".cast(TimestampType), $"id", $"bytes")
      .groupBy(window($"timestamp", "1 hour").as("timestamp"), $"id")
      .agg(sum($"bytes").as("value"))
      .withColumn("type", lit("user_total_bytes"))
      .withColumn("timestamp", $"timestamp.start")
      .select($"timestamp", $"id", $"value", $"type")
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }
  def computeQuotaByMAIL(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .select($"email", $"bytes", $"quota",$"timestamp".cast(TimestampType))
      .groupBy($"email",$"quota",window($"timestamp", "1 hour").as("timestamp"))
      .agg(sum($"bytes").as("usage"))
      .filter($"usage"> $"quota")
      .withColumn("timestamp", $"timestamp.start")
      .select($"email",$"usage",$"quota",$"timestamp")
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()
  }

 override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit = {
   dataFrame
     .write
     .partitionBy("year", "month", "day", "hour")
     .format("parquet")
     .mode(SaveMode.Overwrite)
     .save(s"$storageRootPath\\historical")
 }

  def main (args: Array[String]): Unit = {
  val argsTime = "2021-02-20T21:00:00Z"

    val antennaDF= readFromStorage("C:\\Users\\evaes\\Documents\\GitHub\\Entrega_data_processing_eva\\exerciseEva\\src\\main\\resources\\practica\\data", OffsetDateTime.parse(argsTime))
    val userMetadataDF = readAntennaMetadata("jdbc:postgresql://34.76.48.93:5432/postgres", "user_metadata", "postgres", "keepcoding")
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF,userMetadataDF).cache()

    val aggByANTDF: Unit =  computeBytesCountByANT(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "bytes_hourly", "postgres", "keepcoding")
    val aggByAPPDF: Unit= computeBytesCountByAPP(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "bytes_hourly", "postgres", "keepcoding")
    val aggByUSERDF: Unit = computeBytesCountByUSER(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "bytes_hourly", "postgres", "keepcoding")
    val UserOverQuota: Unit = computeQuotaByMAIL(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "user_quota_limit", "postgres", "keepcoding")
    writeToStorage(antennaDF, "C:\\Users\\evaes\\Documents\\GitHub\\Entrega_data_processing_eva\\exerciseEva\\src\\main\\resources\\practica")


  }
}
