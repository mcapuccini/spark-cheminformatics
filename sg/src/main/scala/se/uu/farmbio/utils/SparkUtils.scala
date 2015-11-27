package se.uu.farmbio.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j._
import org.apache.spark.rdd._
import org.apache.spark._


object SparkUtils {


  /**
   * shutdown-function for explicitly shutting down the SparkContext
   */
  def shutdown(sc: SparkContext){
    sc.stop();
  }

  /**
   * init method that simplify initialization of the SparkContext
   * @param master    The master, default is null, meaning running on cluster. Use "local" or "local[N]" for running locally
   * @param appName   The application name, default is "SparkApp"
   * @return          The created SparkContext, given the input parameters
   */
  def init(master:String =null, appName: String="SparkApp"): SparkContext = {

      val conf = new SparkConf()
      .setAppName(appName)
      .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
      
      if (master != null){
        conf.setMaster(master);  
      }
      return new SparkContext(conf)
  }

  /**
   * Utility function that sets the log-levels to Level.OFF to reduce the extreme 
   * verbosity of the Log4j. 
   */
  def silenceSpark() {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("spark").setLevel(Level.OFF)
    Logger.getLogger("io.netty").setLevel(Level.OFF);
    Logger.getLogger("freemarker").setLevel(Level.OFF);
  }
}