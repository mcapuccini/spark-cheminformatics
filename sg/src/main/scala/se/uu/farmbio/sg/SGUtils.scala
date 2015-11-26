package se.uu.farmbio.sg

import scala.collection.JavaConversions._
import scala.collection.immutable.Map
import scala.collection.immutable.SortedMap
import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.openscience.cdk.exception.InvalidSmilesException
import org.openscience.cdk.interfaces.IAtomContainer
import org.openscience.cdk.signature.AtomSignature

import se.uu.farmbio.sg._
import se.uu.farmbio.sg.types._
import se.uu.farmbio.sg.exceptions._

/**
 * @author staffan
 * 
 * SGUtils groups Signature Generation functionality. Datatypes used as input and output can be found in uu.farmbio.common.sg.types
 */
object SGUtils {

  /**
   * atoms2Vectors take an RDD of molecules (without any outcome class/regression value) and
   * turns each molecule into a Vector, using the given signatureUniverse.
   */
  /*
  def atoms2Vectors(
      molecules: RDD[IAtomContainer], 
			signatureUniverse: RDD[Sig2ID_Mapping],
			h_start: Int, 
			h_stop: Int): RDD[Vector] = {
    atoms2Vectors_carryData(molecules.map((Unit,_)), signatureUniverse, h_start, h_stop).
      map(_._2)
  }
  
  atoms2Vectors_carryData[T: ClassTag](
      molecules: RDD[(T, IAtomContainer)], 
			signatureUniverse: RDD[Sig2ID_Mapping],
			h_start: Int, 
			h_stop: Int): RDD[(T,Vector)] = {
    
  }
      */
  
  /**
   * atom2LP take a unique molecule and compute the corresponding LabeledPoint
   * in a pre-defined signatureUniverse. This will not add new signatures to the 
   * universe, i.e. should only be used in cases of testing models
   * @param molecule      The molecule stored as an IAtomContainer
	 * @param label 				The decision-class or regression value of this molecule
	 * @param signatureUniverse The "Universe" of allowed signatures (new signatures will not be used!)
	 * @param h_start       The starting height of signature generation
	 * @param h_stop        The stopping height of signature generation
	 * @return							The LabeledPoint for the corresponding molecule
   */
  def atom2LP(
      molecule: IAtomContainer, 
			label: Double,
			signatureUniverse: RDD[Sig2ID_Mapping],
			h_start: Int, 
			h_stop: Int): LabeledPoint={
    
    //SignatureRecord = Map[Signature: String, #occurrences: Int]
    val sigRec: SignatureRecord = atom2SigRecord(molecule, h_start, h_stop);

    // Sig2ID_Mapping = (Signature, Sig ID)
    val sigRecID = sigRec.flatMap{case((signature, occ))=>
      val sigID = signatureUniverse.lookup(signature);
      if(sigID.length == 1){
        Some((sigID(0), occ));
      }
      else if(sigID.length > 1){
        throw new RuntimeException("Something is wrong in ");
      }
      else{
        None; //(signatureUniverse.lookup(signature)(0), occ) // This lookup might be slow in distributed computing
      }
    }
    
    val arr_length = sigRecID.size;
		var key_arr = Array.ofDim[Int](arr_length);
		var value_arr = Array.ofDim[Double](arr_length);

		// construct arrays for the signatures
		val sortedMap = SortedMap[Long, Int]() ++ sigRecID; //make the vectors sorted!
		var i = 0;
		sortedMap.map {
		case (key, value) =>
		key_arr(i) = key.toInt;
		value_arr(i) = value;
		i += 1;
		}
		
		//Find the biggest value of signature ID
		var d=0L;
		try{
		  d = signatureUniverse.map { case((signature, sigID)) => sigID }.max +1;
		}
		catch{
		  case e: java.lang.UnsupportedOperationException => //This means that the Sign-mapping was empty!
		}
		
		return LabeledPoint(label, Vectors.sparse(d.toInt, key_arr, value_arr));
    
  }
  
  /**
   * Does the same thing as atoms2LP_carryData, uses the "_carryData"-version. It's not
   * more efficient than using that one!
   */
  def atoms2LP(molecules: RDD[(Double, IAtomContainer)], 
			signatureUniverse: RDD[Sig2ID_Mapping],
			h_start: Int, 
			h_stop: Int): RDD[(LabeledPoint)]={
    
    atoms2LP_carryData(molecules.map{case(label, mol)=>(Unit,label,mol)}, signatureUniverse, h_start, h_stop).
      map{case(_, data) => data}
  }
  
  /**
   * atoms2LP_carryData take an RDD of new molecules och computes signature generation.
   * This function only uses the Signatures given in the "signatureUniverse" parameter.
   * @param molecules		The molecules, their labels and carryData
   * @param signatureUniverse	The mapping of signatures->id that will be used
   * @param h_start			The start of signature generation
   * @param h_stop			The stop of signature generation
   * @param sc					The SparkContext
   * @return 						The LabeledPoint-records generated, with corresponding carryData
   */
  def atoms2LP_carryData[T: ClassTag](
      molecules: RDD[(T, Double, IAtomContainer)], 
			signatureUniverse: RDD[Sig2ID_Mapping],
			h_start: Int, 
			h_stop: Int): RDD[(T,LabeledPoint)]={
    
    val rdd_sigRecordDecision = molecules.map{case((data: T, label: Double, mol: IAtomContainer))=>
      (data, (label, atom2SigRecord(mol, h_start, h_stop)))};
    //val rdd_withID = rdd_sigRecordDecision.zipWithUniqueId;
    val rdd_withFuture_vectors = getFeatureVectors_carryData(rdd_sigRecordDecision, signatureUniverse);
    
    //Find the biggest d (dimension)
		var d=0L;
		try{
		  d = signatureUniverse.map { case((_, sigID)) => sigID }.max +1;
		}
		catch{
		  case e: java.lang.UnsupportedOperationException => //This means that the Sign-mapping was empty!
		}
    
    sig2LP_carryData(rdd_withFuture_vectors, d);
  }
  
  /**
   * atoms2LP_UpdateSignMapCarryData performs signature generation on new molecules, but
   * in contrast to the non "UpdateSignMap" functions, it will add new signatures to the
   * "signature universe". 
   */
  def atoms2LP_UpdateSignMapCarryData[T: ClassTag](molecules: RDD[(T, Double, IAtomContainer)], 
      old_signMap: RDD[Sig2ID_Mapping], 
      h_start: Int, 
      h_stop: Int): (RDD[(T,LabeledPoint)],RDD[Sig2ID_Mapping])={
    
    // Perform Signature Generation on all molecules 
     val rdd_sigRecordDecision = molecules.map{case((data: T, label: Double, mol: IAtomContainer))=>
      (data, (label, atom2SigRecord(mol, h_start, h_stop)))};
    
    //Pick out all signatures
      val allSignatures: RDD[String] = rdd_sigRecordDecision.flatMap { 
		  case (_: T, (_: Double, theMap: Map[String, Int])) => 
		theMap.keySet }.distinct

		// Find the highest previous signatureID:
		val higestID = 
		try{
		  old_signMap.map { case(_:String, id: Long) => id }.max;
		}
		catch{
		  case _: java.lang.UnsupportedOperationException => 0L // meaning no old signatures pesent
		} 
		
		// Find the new Sig2ID_Mapping with both old and new signatures
		val new_signMap = allSignatures.subtract(old_signMap.map { case(sign: String, _: Long) => sign }).
		zipWithUniqueId.map { case(sign: String, old_id: Long) => (sign, old_id+higestID+1) } ++ old_signMap;
		
		val d = new_signMap.map{case (_,id) => id}.max +1 ;
		
		// Generate feature vectors with the new Sig2ID_Mapping
    val rdd_withFuture_vectors = getFeatureVectors_carryData(rdd_sigRecordDecision, new_signMap);
    
    (sig2LP_carryData(rdd_withFuture_vectors, d), new_signMap); 
      
  }
  
	/**
	 * atom2SigRecordDecision computes signatures for a molecule, for usecase where you wish to store the class/regression-value
	 * together with the molecule itself.
	 * @param molecule      The molecule stored as an IAtomContainer
	 * @param decisionClass The decision-class or regression value of this molecule
	 * @param h_start       The starting height of signature generation
	 * @param h_stop        The stopping height of signature generation
	 * @return              SignatureRecordDecision = (Decision-class, Map[Signature, Num occurrences]) 
	 */
	def atom2SigRecordDecision (
			molecule: IAtomContainer, 
			decisionClass: Double, 
			h_start: Int, 
			h_stop: Int
			): 
				SignatureRecordDecision = {
			(decisionClass, atom2SigRecord(molecule, h_start, h_stop));
	}


	/**
	 * atom2SigRecord computes signatures for a molecule, used when class/regression-info doesn't need to be sent with the molecule.
	 * @param molecule      The molecule stored as an IAtomContainer
	 * @param h_start       The starting height of signature generation
	 * @param h_stop        The stopping height of signature generation
	 * @return              SignatureRecord = Map[Signature, Num occurrences]
	 */
	def atom2SigRecord(
			molecule: IAtomContainer,
			h_start: Int, 
			h_stop: Int
			): 
				SignatureRecord = { 
			try {

				// Map is [Signature, count]
				var signature_map = Map.empty[String, Int];
				val h_stop_new = Math.min(molecule.getAtomCount - 1, h_stop); //In case a too big h_stop is set

				for (atom <- molecule.atoms()) {
					for (height <- h_start to h_stop_new) {

						val atomSign = new AtomSignature(molecule.getAtomNumber(atom), height, molecule);
						val canonicalSign = atomSign.toCanonicalString();

						// Check if that signature has been found before for this molecule, update the quantity in such case
						val quantity = signature_map.getOrElse(canonicalSign, -1);

						if (quantity == -1) {
							signature_map += (canonicalSign -> 1);
						} else {
							signature_map += (canonicalSign -> (quantity + 1));
						}
					}
				}
				return signature_map;
			} 
			catch {
			  case ex : Throwable => throw new SignatureGenException("Unknown exception occured (in 'atom2SigRecord'), exception was: " + ex);
			}
	}

	/**
	 * sig2ID_carryData is for transforming an RDD with SignatureRecordDecision-records into the universe of signatures
	 * mapped into corresponding ID's and the records transformed into using the ID's instead. This method can carry
	 * additional data such as record-ID or other info that might be useful. The result is the tuple: 
	 * (RDD[(T, SignatureRecordDecision_ID)], RDD[Sig2ID_Mapping]), where the first element is the SignatureRecordDecision now
	 * encoded with ID's instead of signatures and the carried data of type T. The second element is the universe of signatures used.
	 * 
	 * @param rdd     The RDD should have the form of (T, SignatureRecordDecision), where T can be any object or set of objects
	 * @return        The tuple of (records using ID, ID_mapping)
	 */
	def sig2ID_carryData[T: ClassTag](rdd: RDD[(T, SignatureRecordDecision)]): (RDD[(T, SignatureRecordDecision_ID)], RDD[Sig2ID_Mapping]) ={  

		// Compute the unique Signature -> ID mapping. 
		val sig2ID_mapping: RDD[Sig2ID_Mapping] = rdd.flatMap { 
		  //case (carryData: T, (value: Double, map: Map[String,Int])) => 
		  case (_, (_, map)) => 
		    map.keySet 
		}.
		//map { case (signature: String, count: Int) => (signature) }.
		distinct.
		zipWithUniqueId;

		(getFeatureVectors_carryData(rdd, sig2ID_mapping), sig2ID_mapping);

	}


	/**
	 * Take the rdd_input (your data-records) and the unique mapping of Signature->ID
	 * Transform data-records to use the Signature->ID mapping
	 * 
	 * @param rdd              The RDD containing the data using Signatures
	 * @param uniqueSignatures The unique signatures (mapping Signature->ID)
	 * @return    RDD[SignatureRecordDecision_ID] (Value/Class, Map[Signature_ID, #occurrences])
	 */
	private def getFeatureVectors_carryData[T: ClassTag](rdd: RDD[(T,SignatureRecordDecision)],
			uniqueSignatures: RDD[Sig2ID_Mapping]): RDD[(T, SignatureRecordDecision_ID)] = {

		// Add a unique ID to each record: 
		val req_with_id: RDD[((T, SignatureRecordDecision), Long)] = rdd.zipWithUniqueId;
	  // So here we store (T, (Double, Map[String, Int])) -> ID

	  // Sig2ID_Mapping = (String, Long)
	  //((Double, Map[String, Int]), Long) = (SignatureRecordDecision, Long)
	  // transform to: (String, (Double, Int, Long)) format for joining them!

	  // Expand the Maps 
	  val expanded_rdd = req_with_id.flatMap {
	  //case((carry: T, (dec: Double, mapping: Map[String, Int])), molID: Long ) =>
	    case((carry, (dec, mapping)), molID ) =>
		    mapping.toSeq. //get the map as Seq((String,Int), Int)
		      map {
		        case (signature: String, count: Int) =>
		          (signature, (count, molID))
		      } // result: (signature, (#of occurrences of this signature, MoleculeID))
	  }

	  //: RDD[(String,((Double, Int, Long), Long))]
	  // combine the unique_ID with expanded mapping
	  val joined = expanded_rdd.join(uniqueSignatures);
	  // ((signature,height), ((#occurrences, MoleculeID), Signature_ID))

	  // (MoleculeID, Signature_ID, #occurrences, value)
	  val res_rdd: RDD[(Long, (Long, Int))] = joined.map {
	    case (signature, ((count, mol_ID), sign_ID)) =>
	      (mol_ID, (sign_ID, count));
	    }

	  // Group all records that belong to the same molecule to new row
	  val result: RDD[(Long, Map[Long, Int])] = res_rdd.aggregateByKey(Map.empty[Long, Int])(
			seqOp = (resultType, input) => (resultType + (input._1 -> input._2)),
			combOp = (resultType1, resultType2) => (resultType1 ++ resultType2)
		);

	  //(Double, Map[Long, Int]);
	  // add the carry and decision/regression value
	  req_with_id.map{//case ((carry: T, (dec: Double, _: Map[String, Int])), molID: Long)=>
	    case ((carry, (dec, _)), molID)=>
	      (molID, (carry, dec))}. join(result). map { //case(molID: Long, ((carry: T, dec: Double),(mapping: Map[Long, Int])))=>
	        case(molID, ((carry, dec),(mapping)))=>
	          (carry, (dec, mapping))
	  }


	}


	/**
	 * sig2ID is for transforming an RDD with SignatureRecordDecision-records into the universe of signatures
	 * mapped into corresponding ID's and the records transformed into using the ID's instead. The result is the tuple: 
	 * (RDD[(T, SignatureRecordDecision_ID)], RDD[Sig2ID_Mapping]), where the first element is the SignatureRecordDecision now
	 * encoded with ID's instead of signatures. The second element is the universe of signatures used.
	 * 
	 * @param rdd     The RDD of SignatureRecordDecision-records
	 * @return        The tuple of (records using ID, ID_mapping)
	 */
	def sig2ID(rdd: RDD[SignatureRecordDecision]): (RDD[SignatureRecordDecision_ID], RDD[Sig2ID_Mapping]) ={  

		/*
		// Get the unique Signature -> ID mapping
		val sig2ID_mapping: RDD[Sig2ID_Mapping] = rdd.flatMap { case (_: Double, map: Map[String,Int]) => map.toSeq }.
				map { case (signature: String, count: Int) => (signature) }.
				distinct.
				zipWithUniqueId();

				(getFeatureVectors(rdd, sig2ID_mapping), sig2ID_mapping)

		 */
	  		// Get the unique Signature -> ID mapping
		val sig2ID_mapping: RDD[Sig2ID_Mapping] = rdd.flatMap { case (_: Double, map: Map[String,Int]) => map.toSeq }.
				map { case (signature: String, count: Int) => (signature) }.
				distinct.
				zipWithUniqueId();

				(getFeatureVectors(rdd, sig2ID_mapping), sig2ID_mapping)
	}


	/**
	 * Take the rdd_input (your data-records) and the unique mapping of Signature->ID
	 * Transform data-records to use the Signature->ID mapping
	 * @param rdd              The RDD containing the data using Signatures
	 * @param uniqueSignatures The unique signatures (mapping Signature->ID)
	 * @return    RDD[SignatureRecordDecision_ID] (Value/Class, Map[Signature_ID, #occurrences]
	 */
	private def getFeatureVectors(rdd: RDD[SignatureRecordDecision],
			uniqueSignatures: RDD[Sig2ID_Mapping]): RDD[SignatureRecordDecision_ID] = {

		// Add a unique ID to each record: 
		val req_with_id: RDD[(SignatureRecordDecision, Long)] = rdd.zipWithUniqueId();


	  // Sig2ID_Mapping = (String, Long)
	  //((Double, Map[String, Int]), Long) = (SignatureRecordDecision, Long)
	  // transform to: (String, (Double, Int, Long)) format for joining them!

	  // Expand the Maps 
	  val expanded_rdd = req_with_id.flatMap {
	    case((dec: Double, mapping: Map[String, Int]), id: Long ) =>
	    {
		    mapping.toSeq. //get the map as Seq((String,Int), Int)
		      map {
		      case (signature: String, count: Int) =>
		        (signature, (dec, count, id))
		      }; // result: (signature, (value/class, #of occurrences of this signature, MoleculeID))
	    }
	  }

	  //: RDD[(String,((Double, Int, Long), Long))]
	  // combine the unique_ID with expanded mapping
	  val joined = expanded_rdd.join(uniqueSignatures);
	  // ((signature,height), ((value/class, #occurrences, MoleculeID), Signature_ID))

	  // (MoleculeID, Signature_ID, #occurrences, value)
	  val res_rdd: RDD[(Long, (Long, Int, Double))] = joined.map {
	    case (signature, ((value, count, mol_ID), sign_ID)) =>
	      (mol_ID, (sign_ID, count, value));
	  }

	  // Group all records that belong to the same molecule to new row
	  val result: RDD[(Double, Map[Long, Int])] = res_rdd.aggregateByKey((0.0, Map.empty[Long, Int]))(
			seqOp = (resultType, input) => (input._3, resultType._2 + (input._1 -> input._2)),
			combOp = (resultType1, resultType2) => (resultType1._1, resultType1._2 ++ resultType2._2)).
			map { x => x._2 }

	  result
	}

	
	/**
	 * sig2Vectors transfers records with ID into Vector-objects
	 */
	def sig2Vectors(in: RDD[SignatureRecord_ID], dim: Long=0):
	    RDD[Vector] = {
	  val sc = in.context;
	  
	  if(in.isEmpty) {
	    return sc.emptyRDD;
	  }
	  
	  val sparse_vectors = in.map { (record) =>

		  val arr_length = record.size;
		  var key_arr = Array.ofDim[Int](arr_length);
		  var value_arr = Array.ofDim[Double](arr_length);

		  // construct arrays for the signatures
		  val sortedMap = SortedMap[Long, Int]() ++ record; //make the vectors sorted!
		  var i = 0;
		  sortedMap.map {
		    case (key, value) =>
		      key_arr(i) = key.toInt;
		      value_arr(i) = value;
		      i += 1;
		    }
		  (key_arr, value_arr);
		}
	  
	  // Determine number of features.
		val d = if(dim>0){
		  dim;
		}
		else {
				sparse_vectors.cache;
				sparse_vectors.map {
				case (indices, values) =>
				  indices.max
				}.reduce(math.max) + 1
		}
	  
	  sparse_vectors.map{case(indices, values) =>
	    Vectors.sparse(d.toInt, indices, values)
	  }
	}
	
	/**
	 * Transfers records with IDs into LabeledPoint-objects to allow machine learning on them
	 * @param in    The RDD[SignatureRecordDecision_ID] with data
	 * @return   RDD[LabeledPoint]
	 */
	def sig2LP(in: RDD[SignatureRecordDecision_ID], dim: Long=0): 
	    RDD[LabeledPoint] = {
		// type SignatureRecord = (Double, Map[Long,Int]);  
		val data = in;
		val sc = in.context;

		if (data.isEmpty()) {
			return (sc.emptyRDD)
		}

		val parsed_data = data.map { (record) =>
		val label = record._1;
		val arr_length = record._2.size;
		var key_arr = Array.ofDim[Int](arr_length);
		var value_arr = Array.ofDim[Double](arr_length);

		// construct arrays for the signatures
		val sortedMap = SortedMap[Long, Int]() ++ record._2; //make the vectors sorted!
		var i = 0;
		sortedMap.map {
		case (key, value) =>
		key_arr(i) = key.toInt;
		value_arr(i) = value;
		i += 1;
		}
		(label, key_arr, value_arr);
		}

		// Determine number of features.
		val d = if(dim>0){
		  dim;
		}
		else {
				parsed_data.cache;
				parsed_data.map {
				  case (label, indices, values) =>
				    indices.max
				}.reduce(math.max) + 1
		}

		parsed_data.map {
		  case (label, indices, values) =>
		    LabeledPoint(label, Vectors.sparse(d.toInt, indices, values))
		}

	}


	/**
	 * Transfers records with IDs into LabeledPoint-objects to allow machine learning used on them
	 * 
	 * @param in     The RDD[SignatureRecordDecision_ID] with data
	 * @return      RDD[LabeledPoint]
	 */
	def sig2LP_carryData[T: ClassTag](rdd: RDD[(T, SignatureRecordDecision_ID)], dim: Long=0): 
	    RDD[(T,LabeledPoint)] = {
		//type SignatureRecordDecision_ID = (Double, Map[Long, Int]);
		val sc = rdd.context;
	  
		if (rdd.isEmpty()) {
			return (sc.emptyRDD)
		}

		val parsed_data = rdd.map { //case(carryData: T, record: SignatureRecordDecision_ID) =>
		  case(carryData, record) =>
		val label = record._1;
		val arr_length = record._2.size;
		var key_arr = Array.ofDim[Int](arr_length);
		var value_arr = Array.ofDim[Double](arr_length);

		// construct arrays for the signatures
		val sortedMap = SortedMap[Long, Int]() ++ record._2; //make the vectors sorted!
		var i = 0;
		sortedMap.map {
		case (key, value) =>
		key_arr(i) = key.toInt;
		value_arr(i) = value;
		i += 1;
		}
		(carryData,(label, key_arr, value_arr));
		}

		// Determine number of features.

		val d = if(dim>0){
		  dim;
		}
		else {
				parsed_data.cache;
				parsed_data.map {
				case (_, (_, indices, _)) =>
				indices.max
				}.reduce(math.max) + 1
		}

		val rdd_labeledPoint = parsed_data.map {
		case (data, (label, indices, values)) =>
		(data, LabeledPoint(label, Vectors.sparse(d.toInt, indices, values)))
		}

		return rdd_labeledPoint;

	}


	/**
	 * Writes the RDD[LabeledPoint] to a text-file
	 * @param data    The data in RDD[LabeledPoint] format
	 * @param dir     The directory where the data should be stored 
	 */
	def saveAsLibSVMFile(data: RDD[LabeledPoint], dir: String){
		saveAsLibSVMFileGeneral(data, dir);
	}

	/**
	 * Loads a LibSVM file into an RDD from file
	 * 
	 * @param sc    The SparkContext running
	 * @param path  The path to the LibSVM-file on disc
	 * @return      The RDD[LabeledPoint]
	 */
	def loadLibSVMFile(sc: SparkContext, path: String): RDD[LabeledPoint]={
			loadLibSVMFileGeneral(sc, path);
	}

	/**
	 * Save the ID -> Signature mapping (type Sig2ID_Mapping = (String, Long)). They will be stored
	 * in the format: ID\tSignature
	 * 
	 * @param mapping   mapping in: RDD[(Signature, ID)] format
	 * @param dir       The directory to save it in
	 */
	def saveSign2IDMapping(mapping: RDD[Sig2ID_Mapping], dir: String){ 
		saveSign2IDMappingGeneral(mapping, dir);
	}


	/**
	 * loads the ID-> Signature mapping from file
	 * 
	 * @param dir   The file-path to the stored file
	 * @param sc    The running SparkContext
	 * @return      The RDD[Sign2ID_Mapping] that is the universe of Signature->ID 
	 */
	def loadSign2IDMapping(sc: SparkContext, dir: String): RDD[Sig2ID_Mapping]={
			loadSign2IDMappingGeneral(sc, dir);
	}

}