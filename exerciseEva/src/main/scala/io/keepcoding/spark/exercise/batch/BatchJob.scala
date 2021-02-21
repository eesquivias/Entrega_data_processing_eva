package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercise.batch.batchJobImpl.{computeBytesCountByANT, computeBytesCountByAPP, computeBytesCountByUSER}

import java.sql.Timestamp
import java.time.OffsetDateTime



import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, app: String, id: String, antenna_id: String, bytes: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaStreamingDF: DataFrame, metadataDF: DataFrame): DataFrame

  def computeBytesCountByANT(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit
  //def computeDevicesCountByCoordinates(dataFrame: DataFrame): DataFrame

  def computeBytesCountByAPP(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  //def computeBytesCountByMAIL(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  //def computeErrorAntennaByModelAndVersion(dataFrame: DataFrame): DataFrame

  def computeQuotaByMAIL(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  //def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Unit

  def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcUri, jdbcMetadataTable, aggJdbcTable, aggJdbcErrorTable, aggJdbcPercentTable, jdbcUser, jdbcPassword) = args
    println(s"Running with: ${args.toSeq}")

    val antennaDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    val antennaMetadataDF = enrichAntennaWithMetadata(antennaDF, metadataDF).cache()
    val aggByANTDF: Unit = computeBytesCountByANT(antennaMetadataDF, "jdbc:postgresql://34.76.48.93:5432/postgres", "bytes_hourly", "postgres", "keepcoding")
    //val aggPercentStatusDF = computePercentStatusByID(antennaMetadataDF)
   //val aggErroAntennaDF = computeErrorAntennaByModelAndVersion(antennaMetadataDF)

    val aggByAPPDF: Unit = computeBytesCountByAPP(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "bytes_hourly", "postgres", "keepcoding")
    val aggByUSERDF: Unit = computeBytesCountByUSER(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "bytes_hourly", "postgres", "keepcoding")
   //val aggByMAILDF: Unit = computeBytesCountByMAIL(antennaMetadataDF,"jdbc:postgresql://34.76.48.93:5432/postgres", "user_quota_limit", "postgres", "keepcoding")
   // writeToJdbc(aggByCoordinatesDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)
    //writeToJdbc(aggPercentStatusDF, jdbcUri, aggJdbcPercentTable, jdbcUser, jdbcPassword)
    //writeToJdbc(aggErroAntennaDF, jdbcUri, aggJdbcErrorTable, jdbcUser, jdbcPassword)

   // writeToStorage(antennaDF, storagePath)

    spark.close()
  }

}
