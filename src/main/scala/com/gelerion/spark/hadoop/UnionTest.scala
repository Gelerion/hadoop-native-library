package com.gelerion.spark.hadoop

import com.gelerion.spark.hadoop.utils.AwsCredentialsFetcher
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct

object UnionTest {

  def main(args: Array[String]): Unit = {
    val credentialsProvider = AwsCredentialsFetcher.credentialsProvider()

    val spark = SparkSession.builder().appName("Union")
      .master("local[1]")
      .config("spark.sql.caseSensitive", "true")
      .config("fs.s3a.access.key", credentialsProvider.getAWSAccessKeyId)
      .config("fs.s3a.secret.key", credentialsProvider.getAWSSecretKey)
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.parquet.task.side.metadata", "true")
      .config("spark.hadoop.parquet.summary.metadata.level", "NONE")
      .config("spark.hadoop.parquet.enable.summary-metadata", "false")
      .config("spark.sql.parquet.mergeSchema", value = false)
      .getOrCreate()

    import spark.implicits._

    val in = spark.read.parquet("s3a://denis.shuvalov/cohort_3/dt=2019-07-31/inapps-exact/")
    in.printSchema()
  }
}

