package io.keepcoding.spark.exercise.streaming

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Long, app: String, id: String, antenna_id: String, bytes: Long)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame //datos de entrada

  def readAntennaMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichAntennaWithMetadata(antennaStreamingDF: DataFrame, userMetadataDF: DataFrame): DataFrame

  def computeBytesCountByID(dataFrame: DataFrame): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic, jdbcUri, jdbcMetadataTable, aggJdbcTable, jdbcUser, jdbcPassword, storagePath) = args
    println(s"Running with: ${args.toSeq}")

    val kafkaDF = readFromKafka(kafkaServer, topic)
    //val antennaStreamingDF = parserJsonData(kafkaDF)
    //val userMetadataDF = readAntennaMetadata(jdbcUri, jdbcMetadataTable, jdbcUser, jdbcPassword)
    //val antennaMetadataDF = enrichAntennaWithMetadata(antennaStreamingDF , userMetadataDF)
    //val storageFuture = writeToStorage(antennaMetadataDF, storagePath)
    //val aggByTypeDF = computeBytesCountByID (antennaMetadataDF)
    //val aggFuture = writeToJdbc(aggByTypeDF, jdbcUri, aggJdbcTable, jdbcUser, jdbcPassword)

    //Await.result(Future.sequence(Seq(aggFuture, storageFuture)), Duration.Inf)

    spark.close()
  }

}
