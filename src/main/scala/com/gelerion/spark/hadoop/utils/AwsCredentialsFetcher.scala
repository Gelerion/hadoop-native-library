package com.gelerion.spark.hadoop.utils

import java.io.File

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.profile.{ProfileCredentialsProvider, ProfilesConfigFile}

import scala.io.Source

object AwsCredentialsFetcher {
  def credentialsProvider(): AWSCredentials =
    new ProfileCredentialsProvider(new ProfilesConfigFile(s"${System.getProperty("user.home")}/.aws/credentials"), "default").
      getCredentials

  def token(): String = {
    val infoToken = new File(s"${System.getProperty("user.home")}/.ssh/vault-info-token")
    if (infoToken.exists()) {
      val source = Source.fromFile(infoToken)
      val result = source.mkString
      source.close()
      result
    }
    else System.getenv("TOKEN")
  }
}