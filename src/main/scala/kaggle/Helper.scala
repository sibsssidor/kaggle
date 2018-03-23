package kaggle

import java.io.InputStream
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Helper {

  def getResourceAsString(resourcePath: String) =
    {
      val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(resourcePath)
      scala.io.Source.fromInputStream(stream).getLines.mkString
    }

  def getSc(configuration: PropertiesConfiguration) = {
    SparkSession.builder().config(getConf(configuration)).getOrCreate()
  }
  
  def getSc(configuration: SparkConf) = {
    SparkSession.builder().config(configuration).getOrCreate()
  }
  
  def getConf(configuration: PropertiesConfiguration) = {
    new SparkConf()
      .setMaster(configuration.getString("spark.master", configuration.getString("spark.master")))
      .setAppName(configuration.getString("spark.app.name", "mrlocation"))
      .set("spark.executor.uri", configuration.getString("hdfs.base") + configuration.getString("spark.executor.path"))
      .set("spark.executor.memory", configuration.getString("spark.executor.memory"))
      .set("spark.cores.max", configuration.getString("spark.cores.max"))
      .set("spark.sql.tungsten.enabled", "true")
      .setJars(List(configuration.getString("spark.jars")))
      .set("spark.driver.port", configuration.getString("spark.driver.port"))
      .set("spark.driver.host", configuration.getString("spark.driver.host"))
      .set("spark.driver.maxResultSize", configuration.getString("spark.driver.maxResultSize"))
  }
  

}