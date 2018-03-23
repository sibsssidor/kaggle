package kaggle.zillow

import java.text.SimpleDateFormat

import org.apache.commons.configuration.ConfigurationException
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger
import kaggle.Helper

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
    val spark = Helper.getSc(configProperties)

    
    val dataFeaturesCsv = configProperties.getString("data.features.csv")
    val dataResultsCsv = configProperties.getString("data.results.csv")

   
    val dfFeatures = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .csv(dataFeaturesCsv)
        
    val dfResults = spark.read
        .format("csv")
        .option("header", "true") //reading the headers
        .option("mode", "DROPMALFORMED")
        .csv(dataResultsCsv)
        
    val dataMergedPath = configProperties.getString("data.merged.path")
        
    println(dfFeatures.count())
    println(dfResults.count())
    
    dfFeatures.join(dfResults, "parcelid").repartition(1).write.option("header", "true").csv(dataMergedPath + "merged.csv")

    spark.sparkContext.stop()
  }

}