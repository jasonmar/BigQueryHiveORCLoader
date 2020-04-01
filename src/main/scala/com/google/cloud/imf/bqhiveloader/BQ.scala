package com.google.cloud.imf.bqhiveloader

import com.google.api.gax.retrying.RetrySettings
import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.RetryOption
import com.google.cloud.bigquery.{BigQuery, BigQueryError, BigQueryException, BigQueryOptions, Job}
import com.google.cloud.imf.BQHiveLoader
import com.google.cloud.storage.{Storage, StorageOptions}
import org.threeten.bp.Duration

import scala.util.{Failure, Success, Try}

object BQ extends Logging {
  private val NoRetry = RetrySettings.newBuilder()
    .setMaxAttempts(0)
    .setTotalTimeout(Duration.ofHours(24))
    .setInitialRetryDelay(Duration.ofSeconds(30))
    .setRetryDelayMultiplier(2.0d)
    .setMaxRetryDelay(Duration.ofSeconds(300))
    .setInitialRpcTimeout(Duration.ofHours(8))
    .setRpcTimeoutMultiplier(1.0d)
    .setMaxRpcTimeout(Duration.ofHours(8))
    .setJittered(false)
    .build()

  def client(c: Config, creds: GoogleCredentials): BigQuery = BigQueryOptions.newBuilder()
    .setLocation(c.bqLocation)
    .setCredentials(creds)
    .setProjectId(c.bqProject)
    .setHeaderProvider(FixedHeaderProvider.create("user-agent", BQHiveLoader.UserAgent))
    .setRetrySettings(NoRetry)
    .build()
    .getService

  def gcsClient(c: Config, creds: GoogleCredentials): Storage = StorageOptions.newBuilder()
    .setCredentials(creds)
    .setProjectId(c.bqProject)
    .setHeaderProvider(FixedHeaderProvider.create("user-agent", BQHiveLoader.UserAgent))
    .setRetrySettings(NoRetry)
    .build()
    .getService

  def retryOptions(initial: Int = 8, max: Int = 32, mult: Double = 2.0d, total: Int = 120*60) =
  Seq(
    RetryOption.initialRetryDelay(Duration.ofSeconds(initial)),
    RetryOption.maxRetryDelay(Duration.ofSeconds(max)),
    RetryOption.retryDelayMultiplier(mult),
    RetryOption.totalTimeout(Duration.ofSeconds(total))
  )

  private val PartRefMessage = "Cannot add storage to a non-partitioned table with a partition reference"
  private val PartRefHelp =
    """The destination table is not partitioned but a partition decorator is specified on the load
      |job.
      |If the destination table IS NOT partitioned, remove the `--refreshPartition` option or any
      | partition decorator specified on the destination tablespec.
      |If the destination table SHOULD be partitioned, the table will need to be recreated
      | with a tabledef specifying the partition column.
      |""".stripMargin
  private val HelpMessages: Seq[(String,String)] = Seq(
    PartRefMessage -> PartRefHelp
  )

  def getHelpMessage(error: BigQueryError): Option[String] = {
    if (error == null) None
    else {
      val message = Option(error.getMessage)
      message
        .flatMap{msg => HelpMessages.find(x => msg.startsWith(x._1))}
        .map(_._2)
        .orElse(message)
    }
  }

  // block until job completes, using default retry options
  def await(job: Job): Job = job.waitFor(retryOptions():_*)

  def await(maybeJob: Option[Job]): Try[Job] = {
    maybeJob.map(await) match {
      case Some(job) =>
        val status = job.getStatus
        if (status != null){
          logger.info(s"${job.getJobId.getJob} $status")
          if (status.getError == null) Success(job)
          else {
            val helpMsg = getHelpMessage(status.getError).getOrElse("")
            Failure(new BigQueryException(400, helpMsg, status.getError))
          }
        } else Failure(new BigQueryException(404, "JobStatus not set on Job"))
      case _ =>
        Failure(new RuntimeException("Job doesn't exist"))
    }
  }
}
