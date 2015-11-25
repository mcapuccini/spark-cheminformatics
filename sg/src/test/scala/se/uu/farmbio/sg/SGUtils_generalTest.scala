package se.uu.farmbio.sg


import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import se.uu.farmbio.utils.SparkUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.nio.file._
import org.apache.spark.rdd.RDD
import se.uu.farmbio.sg.types._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.SharedSparkContext

/**
 * @author staffan
 */
@RunWith(classOf[JUnitRunner])
class SGUtils_generalTest extends FunSuite with BeforeAndAfter with SharedSparkContext {

	SparkUtils.silenceSpark();
	var tempDirPath: Path =_;
	var absDir: String =_;

	var sign_mapping_rdd: RDD[Sig2ID_Mapping] = null;
	var lp_rdd: RDD[LabeledPoint]=null;
	var lp_rdd_2: RDD[LabeledPoint]=null;

	before{
		tempDirPath = Files.createTempDirectory("Signature_dir").toAbsolutePath();
		absDir = tempDirPath.toString;

		sign_mapping_rdd = sc.makeRDD(Seq(("C",1L), ("COO",2L),("O",3L),("IOOC",4L)));

		lp_rdd = sc.makeRDD(Seq(
				new LabeledPoint(0.0, new DenseVector(Array(1.0,2.0,15.0))),
				new LabeledPoint(1.0, new DenseVector(Array(-.5, 4.0, 30.5))),
				new LabeledPoint(1.0, new DenseVector(Array(-10.5, 4.4, -30.5)))
				));

		lp_rdd_2 = sc.makeRDD(Seq(
				new LabeledPoint(0.0, new SparseVector(20,Array(1,2,15), Array(2.0, 1.0, 1.0))),
				new LabeledPoint(1.0, new SparseVector(20,Array(3,4,5), Array(1.0, 1.0, 5.0))),
				new LabeledPoint(1.0, new SparseVector(20,Array(6,7,8), Array(-3.0, -1.0, 1.0))),
				new LabeledPoint(0.0, new SparseVector(20,Array(9,12,15), Array(12.0, 1.0, 6.0))),
				new LabeledPoint(0.0, new SparseVector(20,Array(1,15,20), Array(2.0, 7.0, 1.0)))
				));
	}

	/**
	 ****************************************************************************************
	 */
	test("saveSign2IDMappingGeneral and loadSign2IDMappingGeneral"){
		// First try to save the thing:
		SGUtils.saveSign2IDMapping(sign_mapping_rdd, absDir+"/signMapping");

		//Then try to load it
		val loaded_sign_mapping = SGUtils.loadSign2IDMapping(sc, absDir+"/signMapping");

		//Check that count == 4
		assert(loaded_sign_mapping.count == 4, 
				"The length before saving/loading should be the same as after");
		assert(loaded_sign_mapping.count == sign_mapping_rdd.count,
				"The length before saving/loading should be the same as after");
	}

	/**
	 ****************************************************************************************
	 */
	test("saveAsLibSVMFileGeneral & loadLibSVMFileGeneral"){
		// Testing with DenceVector representation of the LabeledPoint-implementation

		SGUtils.saveAsLibSVMFile(lp_rdd, absDir+"/data.libsvm");
		SGUtils.saveAsLibSVMFile(lp_rdd_2, absDir+"/data2.libsvm");

		//Load the stuff
		val lp_data1 = SGUtils.loadLibSVMFile(sc, absDir+"/data.libsvm");
		val lp_data2 = SGUtils.loadLibSVMFile(sc, absDir+"/data2.libsvm");

		// check it
		assert(lp_data1.count == 3, "The size should be the same");
		assert(lp_data2.count == 5, "The size should be the same");
	}

	test("Ending test.."){
		// works as tare-down of the complete file
		SparkUtils.shutdown(sc);
		assert(true);
	}
}