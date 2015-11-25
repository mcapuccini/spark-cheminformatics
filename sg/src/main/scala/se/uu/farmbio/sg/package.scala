package se.uu.farmbio

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import java.lang.Long

import uu.farmbio.sg.types._

/**
 * @author staffan
 * 
 * This package object is made for grouping general code which is common for both SGUtils and SGUtilsWHeight.
 * 
 */
package object sg {

  /**
   * Writes the RDD[LabeledPoint] to a text-file
   * @param data    The data in RDD[LabeledPoint] format
   * @param dir     The directory where the data should be stored 
   */
  protected[sg] def saveAsLibSVMFileGeneral(data: RDD[LabeledPoint], dir: String){
    MLUtils.saveAsLibSVMFile(data, dir);
  }
  
  /**
   * Loads a LibSVM file into an RDD from file
   */
  protected[sg] def loadLibSVMFileGeneral(sc: SparkContext, path: String): RDD[LabeledPoint]={
    MLUtils.loadLibSVMFile(sc, path);
  }
  
  /**
   * Writes the Signature->ID mapping to file
   */
  protected[sg] def saveSign2IDMappingGeneral(mapping: RDD[Sig2ID_Mapping], dir: String){
    mapping.map{case(sig: String, id: scala.Long)=> ""+id+"\t"+sig }.cache.saveAsTextFile(dir);
  }

  /**
   * loads Sign2ID_Mapping from a file back into an RDD
   */
  protected[sg] def loadSign2IDMappingGeneral(sc: SparkContext, dir: String): RDD[Sig2ID_Mapping]={
    sc.textFile(dir).map { line => val split = line.split('\t'); (split(1), Long.valueOf(split(0))) }
  }
  
 
}
