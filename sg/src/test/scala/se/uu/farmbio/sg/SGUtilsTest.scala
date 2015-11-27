package se.uu.farmbio.sg

import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.openscience.cdk.smiles.SmilesParser
import org.openscience.cdk.silent.SilentChemObjectBuilder
import se.uu.farmbio.sg.types._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import se.uu.farmbio.utils.SparkUtils
import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.linalg.DenseVector
import java.nio.file._
import scala.collection.immutable.Map
import org.openscience.cdk.interfaces.IAtomContainer


object DataForTest{
  
  val trueListOfSignatures = List("C", "CO", "COO", "O", "IOOC", "OKO", "I", "CC", "p[C]", "CpC");
  
  def getSignRecDec_carry(sc: SparkContext): RDD[(Int,SignatureRecordDecision)] = {
    sc.parallelize(Seq(
			(1, (1.0, Map(("C",1), ("COO",2),("I",3),("IOOC",4)))),
			(3, (0.0, Map(("C",2), ("CO",2),("OKO",1)))),
			(42, (0.0, Map(("C",3), ("COO",1),("O",3)))),
			(1337, (1.0, Map(("C",1), ("CO",1),("O",3),("CC",5), ("p[C]", 1),("CpC",1))))
			));
  }
  
  def getSig2ID_carry(rdd: RDD[(Int,SignatureRecordDecision)]) = {
    SGUtils.sig2ID_carryData(rdd)
  }
  
  def getSig2ID_withOutCarry(rdd: RDD[SignatureRecordDecision]) = {
    SGUtils.sig2ID(rdd);
  }
}

/**
 * @author staffan
 */
@RunWith(classOf[JUnitRunner])
class SGUtilsTest extends FunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	SparkUtils.silenceSpark();
	
	
	test("atoms2Vectors and atoms2Vectors_carryData"){
	  testAtoms2Vectors();
	  def testAtoms2Vectors()={
	    
	    // For empty mappings
	    val emptySigMapping: RDD[(String, Long)] = sc.parallelize(Seq());
	  
	    val sp = new SmilesParser(SilentChemObjectBuilder.getInstance());
		  val mol = sp.parseSmiles("C=O");
		  val mol_rdd = sc.parallelize(Seq(mol));
	    //println("mol_rdd.length: " + mol_rdd.count);
	    val rdd_vec_0length = SGUtils.atoms2Vectors(mol_rdd, emptySigMapping, 0, 2);

	    assert(rdd_vec_0length.count == 0, "empty sigMapping->no output should be generated");
	    //assert(rdd_vec_0length.collect()(0).size == 1, "The size should be 1 (defined as size +1)");
	    //assert(rdd_vec_0length.collect()(0).toArray == Array(), "should return empty array");
	  
	    // For mappings that contain some data
	    val sigMapping: RDD[(String, Long)] = sc.parallelize(Seq(("[C]", 1L), ("[C]([O])",3L)));
		  val mol2: IAtomContainer = sp.parseSmiles("CO");
		  val mols = sc.parallelize(Seq(mol, mol2));
		  val rdd_vectors_nonEmpty = SGUtils.atoms2Vectors(mols, sigMapping, 0, 2);
		
		  val denseRep = Array(0.0, 1.0, 0.0, 1.0);
		  assert(TestUtils.matchArrays(rdd_vectors_nonEmpty.collect()(1).toDense.toArray,denseRep), 
		    "The correct Vector should be created");
	  
		  //Now test the same thing for the _carryData:
		  val mol_rdd_carry = sc.parallelize(Seq((42,mol), (5,mol2)));
		  val rdd_vec_0length_carry = SGUtils.atoms2Vectors_carryData(mol_rdd_carry, emptySigMapping, 0, 2);
		  assert(rdd_vec_0length_carry.count == 0, "empty sigMapping->no output should be generated");
	    
		  // _carryData with mapping containg data
		  val rdd_v_nonEmpty_carry = SGUtils.atoms2Vectors_carryData(mol_rdd_carry, sigMapping, 0, 2);
		  assert(rdd_v_nonEmpty_carry.count == 2, "correct number of vectors should be returned");
		  assert((rdd_v_nonEmpty_carry.collect()(0)._1 == 42) &&
		      (rdd_v_nonEmpty_carry.collect()(1)._1 == 5), "The carry data should be correct!");
		  assert(TestUtils.matchArrays(rdd_v_nonEmpty_carry.collect()(1)._2.toDense.toArray,denseRep), 
		    "The correct Vector should be created");

	  }
	  
	}
	
	/**
	 * atom2LP should return the correct stuff
	 */
	test("atom2LP should return the correct stuff"){
	  testAtom2LP();
	  def testAtom2LP() {
	   // For empty mappings
	   val emptySigMapping: RDD[(String, Long)] = sc.parallelize(Seq());
	  
	   val sp = new SmilesParser(SilentChemObjectBuilder.getInstance());
		 val mol = sp.parseSmiles("C=O");
	  
	   val lp_should_be_empty = SGUtils.atom2LP(mol, 1.0, emptySigMapping, 0, 2);
	   assert(lp_should_be_empty.label == 1.0, "The given label should be the one returned");
	   assert(lp_should_be_empty.features.size == 0, "The size should be 0 (the vector should be empty)");
	  
	   // For mappings that contain some data
	   val sigMapping: RDD[(String, Long)] = sc.parallelize(Seq(("[C]", 1L), ("[C]([O])",3L)));
		 val mol2 = sp.parseSmiles("CO");
		 val lp_nonEmpty = SGUtils.atom2LP(mol2, -150.0, sigMapping, 0, 2);

		 assert(lp_nonEmpty.label == -150.0, "The given label should be the one returned");
		
		 val denseRep = Array(0.0, 1.0, 0.0, 1.0);
		 assert(TestUtils.matchArrays(lp_nonEmpty.features.toDense.toArray,denseRep), 
		    "The correct LP-s should be created");
	  }
	}
	
	/**
	 * test for atoms2LP and atoms2LP_carryData, both uses the implementation of atoms2LP_carryData
	 * so only this method is explicitly tested. (The atoms2LP only attach bogus data to carry)
	 */
	test("atoms2LP and atoms2LP_carryData"){
	  testAtoms2LP_atoms2LP_carryData();
	  def testAtoms2LP_atoms2LP_carryData(){
	  // For empty mappings
	  val emptySigMapping: RDD[(String, Long)] = sc.parallelize(Seq());
	  val sp = new SmilesParser(SilentChemObjectBuilder.getInstance());
		val mol = sp.parseSmiles("C=O");
		val mol2 = sp.parseSmiles("CO");
		val mol_rdd = sc.parallelize(Seq(("mol1", 1.0, mol),("mol2", 15.0, mol2)));
	  
	  val lps_should_be_empty = SGUtils.atoms2LP_carryData(mol_rdd, emptySigMapping, h_start=0, h_stop=3);
	  val lp_arr = lps_should_be_empty.collect();
	  lp_arr.foreach{case(carryData:String,lp: LabeledPoint)=>
	    if(carryData.equals("mol1")){
	      assert(lp.label==1.0, "The label of mol1 should be the given one");
	    }
	    else if(carryData.equals("mol2")){
	      assert(lp.label==15.0, "The label of mol2 should be the given one");
	    }
	    else{
	      fail("The carryData is faulty!");
	    }
	    assert(lp.features.numActives ==0);
	  }
	  
	  // For NON-EMPTY mappings
	  val sigMapping: RDD[(String, Long)] = sc.parallelize(Seq(("[C]", 1L), ("[C]([O])",2L)));
	  
	  val lps_nonEmpty = SGUtils.atoms2LP_carryData(mol_rdd, sigMapping, h_start=0, h_stop=3).collect;
	  lps_nonEmpty.foreach{case(carryData:String,lp: LabeledPoint)=>
	    if(carryData.equals("mol1")){
	      assert(lp.label==1.0, "The label of mol1 should be the given one");
	      assert(lp.features.numActives ==1);
	      assert(TestUtils.matchArrays(lp.features.toDense.toArray, Array(0.0, 1.0, 0.0)), 
	          "The dense representaiton of the features should match");
	    }
	    else if(carryData.equals("mol2")){
	      assert(lp.label==15.0, "The label of mol2 should be the given one");
	      assert(lp.features.numActives ==2);
	      assert(TestUtils.matchArrays(lp.features.toDense.toArray, Array(0.0, 1.0, 1.0)),
	          "The dense representaiton of the features should match");
	    }
	    else{
	      fail("The carryData is faulty!");
	    }

	  }
	  
	}
	}
	
	
	test("atoms2LP_UpdateSignMapCarryData"){
	  testAtoms2LP_UpdateSignMapCarryData();
	  
	  def testAtoms2LP_UpdateSignMapCarryData(){
	  // Starting with a empty signature Mapping
	  // ------------------------------------------------------------------------
	  val emptySigMapping: RDD[(String, Long)] = sc.parallelize(Seq());
	  val sp = new SmilesParser(SilentChemObjectBuilder.getInstance());
		//val mol = sp.parseSmiles("C=O");
		val mol2 = sp.parseSmiles("CO");
		val mol_rdd = sc.parallelize(Seq(("mol2", 15.0, mol2)));
		val (mol_data, newMapping_someData) = SGUtils.atoms2LP_UpdateSignMapCarryData(mol_rdd, emptySigMapping, h_start=0, h_stop=3);
		
		// Check that the Sig2ID_Mapping is correct
		assert(TestUtils.compareEqualLists(newMapping_someData.map{case(sign: String, _:Long)=>sign}.collect.toList,
		    List("[C]","[O]","[C]([O])","[O]([C])")), 
		    "Should update the sig-universe with the correct signatures");
	  
		// Check that the LP-data is correct
		val (molName: String, lp: LabeledPoint) = mol_data.collect()(0)
		assert(molName.equals("mol2"), "The molecule name should be correct");
		assert(lp.label==15.0, "The label should be correct");
		assert(TestUtils.matchArrays(lp.features.toDense.toArray.filter{x=>if(x!=0) true else false},Array(1.0,1.0,1.0,1.0))); 
		
		// ------------------------------------------------------------------------
	  // Starting with a existing mapping (but not complete cover)
	  val mol = sp.parseSmiles("CC=O");
		val mol_rdd2 = sc.parallelize(Seq(("mol1", -1.0, mol),("mol2", 15.0, mol2)));
		val (mol_data2, newMapping_someData2) = SGUtils.atoms2LP_UpdateSignMapCarryData(mol_rdd2, newMapping_someData, h_start=0, h_stop=3);
	  
		// Check that the mapping is updated and correct
		assert(newMapping_someData2.subtract(newMapping_someData).count > 0, 
		    "new data should have been added");
		assert(newMapping_someData.intersection(newMapping_someData2).count ==4, "The old data should still be there!");
		
		// Check the LP-stuff
		val (molName2: String, lp2: LabeledPoint) = mol_data2.collect()(1)
		assert(lp2.label == 15.0);
		assert(molName2.equals(molName));
		
		assert(TestUtils.matchArraysDiffLength(lp2.features.toDense.toArray, lp.features.toDense.toArray), 
		    "The new and old lp's should match!");
		
		val (molName3: String, lp3: LabeledPoint) = mol_data2.collect()(0)
		assert(lp3.label == -1.0);
		assert(molName3.equals("mol1"));
		assert(lp3.features.toArray.length > 5, 
		    "The other molecule should have a bunch of signatures");
		
		// ------------------------------------------------------------------------
	  // Starting with complete signature coverage
		val (mol_data3, lastMapping) = SGUtils.atoms2LP_UpdateSignMapCarryData(mol_rdd, newMapping_someData2, h_start=0, h_stop=3);
		val (molName4: String, lp4: LabeledPoint) = mol_data3.collect()(0)
		
		//Mapping should not have changed
		assert(TestUtils.compareEqualLists(lastMapping.collect().toList, newMapping_someData2.collect.toList), 
		    "The mapping should not have changed");
		assert(TestUtils.matchArraysDiffLength(lp4.features.toDense.toArray, lp.features.toDense.toArray), 
		    "The new and old lp's should match!");
	}
	}

	/**
	 * atom2SigRecord should return the correct answer
	 */
	test("atom2SigRecord should return the correct answer"){
	  testAtom2SigRecord();
	  def testAtom2SigRecord(){
		  val sp = new SmilesParser(SilentChemObjectBuilder.getInstance());
		  val mol = sp.parseSmiles("C=O");
		  val out: Map[String,Int] = SGUtils.atom2SigRecord(mol, 0, 15);
		  val corrMap = Map("[C]"->1, "[O]"->1, "[C](=[O])"-> 1,"[O](=[C])"->1);
		  assert(corrMap == out, "The output should be correct");

		  // More difficult molecule
		  val paracetamol="CC(=O)Nc1ccc(cc1)O";
		  val paracetamol_IAtomContainer = sp.parseSmiles(paracetamol);

		  val out2: Map[String, Int] = SGUtils.atom2SigRecord(paracetamol_IAtomContainer, 0, 15);
		  assert(out2.size > 10, "Should be possible to generate signatures for valid atom");

		  val out3: (Double, Map[String, Int]) = SGUtils.atom2SigRecordDecision(paracetamol_IAtomContainer, 0.0, 0, 15);
		  assert(out3._1 == 0.0, "The decision should be keept when performing SG");
		  assert(TestUtils.compareEqualLists(out3._2.toList, out2.toList), "The SG should be the same from both functions");
	  }
	}


	/**
	 * sig2ID_carryData & getFeatureVectors_carryData
	 */
	test("sig2ID_carryData & getFeatureVectors_carryData"){
	  testSig2ID_And_getFeatureVectors_CARRY_VERSION();
	  def testSig2ID_And_getFeatureVectors_CARRY_VERSION(){
	  val rdd = DataForTest.getSignRecDec_carry(sc);
    val (rdd_withID_carryData, sig2Id_mapping_carryData) = DataForTest.getSig2ID_carry(rdd)
	  
		// Check that the sig2Id_mapping is correct:
		// num signatures should be 10
		assert(sig2Id_mapping_carryData.count == DataForTest.trueListOfSignatures.length, "The number of unique signatures = 10");

    val signatures = sig2Id_mapping_carryData.map { case(sig: String, _: Long) => sig }.collect;
		assert(TestUtils.compareEqualLists(DataForTest.trueListOfSignatures, signatures.toList), 
				"The found signatures should be correct");
		assert(sig2Id_mapping_carryData.map { case (sign: String, id: Long) => id }.distinct.count == 10, 
				"each SignID-value should be unique!");

		
		// Check that rdd_withID is "correct", good-enough..
		val signRecDec_carry = rdd;
		assert(rdd_withID_carryData.count== signRecDec_carry.count, 
				"The number of molecule-records should be the same before and after");
		// the carryData should be the same..
		val carryData = signRecDec_carry.map{case(myData: Int, _)=> myData}.collect.toList;
		assert(TestUtils.compareEqualLists(List(1,3,42,1337), carryData), 
				"The _carryData should stay the same");

		val signatureUniverseOfIDs = sig2Id_mapping_carryData.map{case(_: String, id: Long)=> id}.collect.toList;
		val theMaps = rdd_withID_carryData.collect.map{case(molID:Int,(dClass: Double,idMap: Map[Long,Int]))=>
		if(molID == 1){
			assert(dClass == 1.0, "Decision-class/reg-value should not be changed");
			assert(idMap.size == 4, "The size of the signatureMap should be correct");
			assert(TestUtils.compareEqualLists(idMap.values.toList, List(1,2,3,4)), "Should not change the num occ");
			assert(idMap.keys.toList.filter { key => signatureUniverseOfIDs.contains(key) }.length == idMap.size,
					"Only SignIDs that is in the 'Universe of Signatures' should exist!");
		}
		else if(molID==3){
			assert(dClass == 0.0, "Decision-class/reg-value should not be changed");
			assert(idMap.size == 3, "The size of the signatureMap should be correct");
			assert(TestUtils.compareEqualLists(idMap.values.toList, List(2,2,1)), "Should not change the num occ");
			assert(idMap.keys.toList.filter { key => signatureUniverseOfIDs.contains(key) }.length == idMap.size,
					"Only SignIDs that is in the 'Universe of Signatures' should exist!");
		}
		else if(molID == 42){
			assert(dClass == 0.0, "Decision-class/reg-value should not be changed");
			assert(idMap.size == 3, "The size of the signatureMap should be correct");
			assert(TestUtils.compareEqualLists(idMap.values.toList, List(3,1,3)), "Should not change the num occ");
			assert(idMap.keys.toList.filter { key => signatureUniverseOfIDs.contains(key) }.length == idMap.size,
					"Only SignIDs that is in the 'Universe of Signatures' should exist!");
		}
		else if(molID == 1337){
			assert(dClass == 1.0, "Decision-class/reg-value should not be changed");
			assert(idMap.size == 6, "The size of the signatureMap should be correct");
			assert(TestUtils.compareEqualLists(idMap.values.toList, List(1,1,3,5,1,1)), "Should not change the num occ");
			assert(idMap.keys.toList.filter { key => signatureUniverseOfIDs.contains(key) }.length == idMap.size,
					"Only SignIDs that is in the 'Universe of Signatures' should exist!");
		}
		else{
			assert(false, "molID was not correct!!")
		}

		}

	  }
	}


	/**
	 * sig2ID & getFeatureVectors
	 */
	test("sig2ID & getFeatureVectors"){
	  testSig2ID_and_getFeatureVectors();
	  def testSig2ID_and_getFeatureVectors(){
	  val rdd_Without = DataForTest.getSignRecDec_carry(sc).map(_._2);
	  val (rdd_withID, sig2Id_mapping) = DataForTest.getSig2ID_withOutCarry(rdd_Without);
	  
	  val signRecDec_carry = DataForTest.getSignRecDec_carry(sc);
    val (rdd_withID_carryData, sig2Id_mapping_carryData) = DataForTest.getSig2ID_carry(signRecDec_carry)

		// Check that the sig2Id_mapping is correct:
		// num signatures should be 10
		assert(sig2Id_mapping.count == DataForTest.trueListOfSignatures.length, "The number of unique signatures = 10");

    val signatures = sig2Id_mapping_carryData.map { case(sig: String, _: Long) => sig }.collect;
		assert(TestUtils.compareEqualLists(DataForTest.trueListOfSignatures, signatures.toList), 
				"The found signatures should be correct");
		assert(sig2Id_mapping.map { case (sign: String, id: Long) => id }.distinct.count == 10, 
				"each SignID-value should be unique!");


		// Check that rdd_withID is "correct", good-enough..
		assert(rdd_withID.count== signRecDec_carry.count, 
				"The number of molecule-records should be the same before and after");
		// the carryData should be the same..
		val carryData = signRecDec_carry.map{case(myData: Int, _)=> myData}.collect.toList;
		assert(TestUtils.compareEqualLists(List(1,3,42,1337), carryData), 
				"The _carryData should stay the same");

		val signatureUniverseOfIDs = sig2Id_mapping.map{case(_: String, id: Long)=> id}.collect.toList;
		val theMaps = rdd_withID.collect.map{case(dClass: Double,idMap: Map[Long,Int])=>
		if(dClass == 1.0){
			if(idMap.size == 4){
				assert(TestUtils.compareEqualLists(idMap.values.toList, List(1,2,3,4)), "Should not change the num occ");
				assert(idMap.keys.toList.filter { key => signatureUniverseOfIDs.contains(key) }.length == idMap.size,
						"Only SignIDs that is in the 'Universe of Signatures' should exist!");
			}
			else if(idMap.size == 6){
				assert(TestUtils.compareEqualLists(idMap.values.toList, List(1,1,3,5,1,1)), "Should not change the num occ");
				assert(idMap.keys.toList.filter { key => signatureUniverseOfIDs.contains(key) }.length == idMap.size,
						"Only SignIDs that is in the 'Universe of Signatures' should exist!");
			}
			else{
				assert(false, "The size of the signatureMap should be correct");
			}

		}
		else if(dClass == 0.0){
			assert(idMap.size == 3, "The size of the signatureMap should be correct");
			assert(TestUtils.compareEqualLists(idMap.values.toList, List(2,2,1)) ||
					TestUtils.compareEqualLists(idMap.values.toList, List(3,1,3)), 
					"Should not change the num occ");
			assert(idMap.keys.toList.filter { key => signatureUniverseOfIDs.contains(key) }.length == idMap.size,
					"Only SignIDs that is in the 'Universe of Signatures' should exist!");
		}
		else{
			assert(false, "dClass was not correct!!")
		}

		}
	}
	}


	/**
	 * sig2LP
	 */
	test("sig2LP"){
	  testSig2LP();
	  def testSig2LP(){
	  val signRecDec = DataForTest.getSignRecDec_carry(sc).map{case(_:Int,x)=>x}; // remove the carryData-part
	  val (rdd_withID, sig2Id_mapping)= SGUtils.sig2ID(signRecDec);
	  
		val lp_out = SGUtils.sig2LP(rdd_withID);
		// SignatureRecordDecision_ID = (Double, Map[Long, Int])
		val firstLP = lp_out.take(1)(0);
		assert(firstLP.features.numActives == 4, "the array should be 4 indicies long");
    //println("\nsig2LP");
		//firstLP.features.toDense.toArray.foreach(println);
		
		// The maps that should be converted:
		val recs = rdd_withID.collect();

		val lp_array = lp_out.collect();
		val matching = lp_array.map{case(lp_rec: LabeledPoint) => 
		recs.filter { rec => TestUtils.matchLPtoSRD_ID(lp_rec, rec) }.size
		}
		assert(TestUtils.compareEqualLists(matching.toList, List(1,1,1,1)),
				"The output from LP should match the input");
	}
	}

	/**
	 * sig2LP_carryData
	 */
	test("sig2LP_carryData"){
	  testSig2LP_carryData();
	  def testSig2LP_carryData(){
	  val signRecDec_carry = DataForTest.getSignRecDec_carry(sc)
	  val (rdd_withID_carryData, _)= SGUtils.sig2ID_carryData(signRecDec_carry);
		val lp_out = SGUtils.sig2LP_carryData(rdd_withID_carryData).collect;
		val recs = rdd_withID_carryData.collect;
		val rec1 = recs.find({case(id: Int, rest)=>if(id==1) true else false}).get._2;
		val rec3 = recs.find({case(id: Int, rest)=>if(id==3) true else false}).get._2;
		val rec42 = recs.find({case(id: Int, rest)=>if(id==42) true else false}).get._2;
		val rec1337 = recs.find({case(id: Int, rest)=>if(id==1337) true else false}).get._2;

		lp_out.map{case(id:Int,lp: LabeledPoint)=>
		if(id==1){
			assert(TestUtils.matchLPtoSRD_ID(lp, rec1), "should match");
		}
		else if(id==3){
			assert(TestUtils.matchLPtoSRD_ID(lp, rec3), "should match");
		}
		else if(id == 42){
			assert(TestUtils.matchLPtoSRD_ID(lp, rec42), "should match");
		}
		else if(id == 1337){
			assert(TestUtils.matchLPtoSRD_ID(lp, rec1337), "should match");
		}
		else{
			assert(false, "the carryData should be non-changed");
		}

		}
	  }
	}

}