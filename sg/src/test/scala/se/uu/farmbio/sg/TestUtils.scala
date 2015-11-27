package se.uu.farmbio.sg

import org.apache.spark.mllib.regression.LabeledPoint
import se.uu.farmbio.sg.types._


/**
 * @author staffan
 */
protected object TestUtils {
	/**
	 * Utility method used for testing
	 */
	def compareEqualLists[T](l1: List[T], l2: List[T]): Boolean ={
			if(l1.diff(l2).length != 0){
				return false;  
			} else {
				true;
			}
	}

	def matchArrays[T](arr1: Array[T], arr2: Array[T]): Boolean={
			if(arr1.length != arr2.length)
				return false;
			else{
				for(i <- 0 to arr1.length-1){
					if(arr1(i) != arr2(i))
						return false;
				}
			}
			true
	}

	def matchArraysDiffLength[T](arr1: Array[T], arr2: Array[T]): Boolean={

	  val (longArr, shortArr) = {
	    if(arr1.length >= arr2.length)
	      (arr1, arr2);
	    else 
	      (arr2,arr1);
	  }
	  
		for(i <- 0 to longArr.length-1){
			if(shortArr.length<= i){
			 // Only have to check the remaing part of the long array (is the rest 0:s?)
			  for(ii <- i to longArr.length -1){
			    if(longArr(i) !=0){
			      return false;
			    }
			  }
			  return true;
			}
			else{
			  if(arr1(i) != arr2(i))
			    return false;
			}
		}

		true
	}

	def matchLPtoSRD_ID(lp: LabeledPoint, srd_id: SignatureRecordDecision_ID): Boolean = {
			if(lp.label != srd_id._1){
				return false;
			}
			val features_fromLP = lp.features.toArray;
			var map_fromLP = Map[Long,Int]();

			for(i <- 0 to lp.features.size-1){
				if(features_fromLP(i) != 0){
					map_fromLP += (i.toLong->features_fromLP(i).toInt)
				}
			}
			compareEqualLists(map_fromLP.toList, srd_id._2.toList);

	}
}