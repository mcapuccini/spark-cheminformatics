package se.uu.farmbio.sg.examples

import se.uu.farmbio.sg.SGUtils
import org.openscience.cdk.smiles.SmilesParser
import se.uu.farmbio.utils.SparkUtils
import org.openscience.cdk.silent.SilentChemObjectBuilder

object SG_Example {
  def main(args: Array[String]): Unit = {
    SparkUtils.silenceSpark();
		val sc = SparkUtils.init("local[*]");
    
		// Generate data, typically done by the parsers-library  
    val sp = new SmilesParser(SilentChemObjectBuilder.getInstance());
		val mol1 = sp.parseSmiles("C1=CC(=CC=C1NC(C)=O)O");
		val mol2 = sp.parseSmiles("COc(c1)cccc1C#N");
		
		// (decision_class, molecule)
		val mols = sc.parallelize(Seq(
		    (1.0, mol1),
		    (0.0, mol2)
		    ));
    
    val moleculesAfterSG = mols.map{case(dec_class, molecule) => SGUtils.atom2SigRecordDecision(molecule, dec_class, h_start=1, h_stop=3)};
    val (result, sig2ID_universe) = SGUtils.sig2ID(moleculesAfterSG);
    val resultAsLP = SGUtils.sig2LP(result);
    // Use the labeledPoints to build a classifier of your own choice
    // ...
    
    
    // when you wish to test molecules, you will only care of the signatures that
    // is already in the "signature universe" of what you have previously seen
    // as you cannot gain anything by using signatures not used in the classifier
    
    val testMol1 = sp.parseSmiles("Clc1ccc(cc1)C(c2ccccc2)N3CCN(CC3)CCOCC(=O)O");
		val testMol2 = sp.parseSmiles("O=C(O)C(c1ccc(cc1)C(O)CCCN2CCC(CC2)C(O)(c3ccccc3)c4ccccc4)(C)C");
		
		val testMols = sc.parallelize(Seq(
		    testMol1,
		    testMol2));
		
		val testLPs = SGUtils.atoms2Vectors(testMols, sig2ID_universe, h_start=1, h_stop=3);
		// Use the previously created classifier to test these molecules.
		// ...
  }
}