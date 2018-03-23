package kaggle.talkdata

import java.sql.Timestamp

import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.Encoders

import kaggle.Helper
import java.util.Calendar
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.rand

object DataPreparation {

  val logger = Logger.getLogger("kaggle.zillow.DataPreparation")

  def main(args: Array[String]) {
    Console.println("Initiating SparkContext...")
    val configProperties = try {
      new PropertiesConfiguration(System
        .getProperty("dataprep.config", "dataprep.properties"))
    } catch {
      case e: ConfigurationException =>
        new PropertiesConfiguration
    }

    println("starting context")
    implicit val spark = Helper.getSc(configProperties)

    val basePath = "/home/flavio/Documents/ML/Kaggle/talkdata/"
    val original = "train.parquet"
    val logNorm = "logNorm.train.parquet"
    val countEnriched = enrichData(basePath, original, "logNorm.train.parquet")
    val split = countEnriched.randomSplit(Array[Double](0.8, 0.2, 0.1))
    split(0).filter("is_attributed = 1")
      .union(split(0).filter("is_attributed = 0").sample(0.01))
      .orderBy(rand(System.currentTimeMillis()))
      .repartition(1).write.parquet(basePath + "train.08.ds.parquet")
    split(1).repartition(1).write.parquet(basePath + "train.02.parquet")
    split(2).repartition(1).write.parquet(basePath + "train.01.parquet")
    enrichTestData(basePath, "test.parquet", "test.enriched.parquet")

    spark.sparkContext.stop()
  }

  def sampleData(basePath: String, file: String, perc: Double)(implicit spark: SparkSession) {
    val parquetFileDF = spark.read.parquet(basePath + file)
    parquetFileDF.sample(perc).repartition(1).write.parquet(basePath + "sample." + file)
  }

  /**
   * Enrich base data set
   *
   * @param basePath
   * @param file
   * @param spark
   */
  def enrichData(basePath: String, inFile: String, outFile: String)(implicit spark: SparkSession) = {
    val parquetFileDF = spark.read.parquet(basePath + inFile)
    parquetFileDF.printSchema()
    val dataEncoder = Encoders.product[RawData]

    val rawData = parquetFileDF.as[RawData](dataEncoder)

    rawData.createOrReplaceTempView("rawData")
    val total = rawData.count()
    val countIp = spark.sql("select ip, cast(log(count(*)/" + total + ") as int) as logCount from rawData group by ip")
    countIp.createOrReplaceTempView("countIp")

    import spark.implicits._

    val timeEnriched = rawData.map(x => {
      val c: Calendar = Calendar.getInstance()
      c.setTimeInMillis(x.click_time.getTime)
      TimeEnriched(x.ip, x.app, x.device, x.os, x.channel,
        x.click_time,
        c.get(Calendar.HOUR_OF_DAY).toByte,
        c.get(Calendar.DAY_OF_WEEK).toByte,
        x.is_attributed)
    })
    timeEnriched.createOrReplaceTempView("timeEnriched")
    val countEnriched = spark.sql("select timeEnriched.ip,logCount,app,device,os,channel,click_time,click_time_hour,click_time_dayw,is_attributed from countIp, timeEnriched where timeEnriched.ip = countIp.ip")
    countEnriched.repartition(1).write.parquet(basePath + "countNormInt." + outFile)
    countEnriched
  }

  def enrichTestData(basePath: String, file: String, outFile: String)(implicit spark: SparkSession) {
    val parquetFileDF = spark.read.parquet(basePath + file)

    parquetFileDF.printSchema()

    val personEncoder = Encoders.product[RawTestData]

    val rawData = parquetFileDF.as[RawTestData](personEncoder)
    val total = rawData.count()

    import spark.implicits._

    val timeEnriched = rawData.map(x => {
      val c: Calendar = Calendar.getInstance()
      c.setTimeInMillis(x.click_time.getTime)
      TestTimeEnriched(x.click_id, x.ip, x.app, x.device, x.os, x.channel,
        x.click_time,
        c.get(Calendar.HOUR_OF_DAY).toByte,
        c.get(Calendar.DAY_OF_WEEK).toByte)
    })

    timeEnriched.createOrReplaceTempView("timeEnriched")
    val countIp = spark.sql("select ip, cast(log(count(*)/" + total + ") as int) as logCount from timeEnriched group by ip")
    countIp.createOrReplaceTempView("countIp")

    val countEnriched = spark.sql("select click_id, countIp.ip, logCount,app,device,os,channel,click_time_hour,click_time_dayw from countIp, timeEnriched where timeEnriched.ip = countIp.ip")
    countEnriched.repartition(1).write.parquet(basePath + outFile)
  }

  def downSampling(basePath: String)(implicit spark: SparkSession) {
    val parquetFileDF = spark.read.parquet(basePath + "train.time.parquet")

    parquetFileDF.printSchema()

    val downSampled = parquetFileDF.filter("is_attributed = 1").union(parquetFileDF.filter("is_attributed = 0").sample(0.1))

    import org.apache.spark.sql.functions.rand

    val shuffledDF = downSampled.orderBy(rand(System.currentTimeMillis()))

    shuffledDF.repartition(1).write.parquet(basePath + "train.downsampled.parquet")
  }

  def devSet(basePath: String)(implicit spark: SparkSession) {
    val parquetFileDF = spark.read.parquet(basePath + "train.time.parquet")

    parquetFileDF.printSchema()

    val downSampled = parquetFileDF.sample(0.2)

    import org.apache.spark.sql.functions.rand

    val shuffledDF = downSampled.orderBy(rand(System.currentTimeMillis()))

    shuffledDF.repartition(1).write.parquet(basePath + "train.dev.parquet")
  }

  case class RawData(
    ip:              Int,
    app:             Int,
    device:          Int,
    os:              Int,
    channel:         Int,
    click_time:      Timestamp,
    attributed_time: Timestamp,
    is_attributed:   Int)

  case class RawTestData(
    click_id:   Int,
    ip:         Int,
    app:        Int,
    device:     Int,
    os:         Int,
    channel:    Int,
    click_time: Timestamp)

  case class TimeEnriched(
    ip:              Int,
    app:             Int,
    device:          Int,
    os:              Int,
    channel:         Int,
    click_time:      Timestamp,
    click_time_hour: Byte,
    click_time_dayw: Byte,
    is_attributed:   Int)

  case class TestTimeEnriched(
    click_id:        Int,
    ip:              Int,
    app:             Int,
    device:          Int,
    os:              Int,
    channel:         Int,
    click_time:      Timestamp,
    click_time_hour: Byte,
    click_time_dayw: Byte)

}