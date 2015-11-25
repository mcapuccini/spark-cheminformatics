package se.uu.farmbio.sg


/**
 * @author staffan
 */
object types {
  
  // -----------------------------------------------
  // For signatures which save Height
  // -----------------------------------------------
  /**
   * SignHeightDecision = (Decision/Regression-value, Map[(Signature, SignatureHeight), #occurrences])
   */
  type SignHeightDecision = (Double, Map[(String, Int), Int]); //Decision, Map[(Signature, Signature_height), #occurrences]
  
  /**
   * SignAndHeight = Map[(Signature, Signature_height), #occurrences]
   */
  type SignAndHeight = Map[(String, Int), Int]; //Map[(Signature, Signature_height), #occurrences]
  
  /**
   * SignatureDF_Format = (Sign_ID, Signature, Height)
   * This is what is stored in the database for each Signature 
   */
  @Deprecated
  type SignatureDF_Format = (Long, String, Int); //Sign_ID, Signature, Height
  /**
   * Sig2ID_Mapping_WHeight = (Sign_ID, Signature, Height)
   * This is what is stored in the database for each Signature 
   */
  type Sig2ID_Mapping_WHeight = (Long, String, Int);

  
  // -----------------------------------------------
  // For Signatures not saving Height!
  // -----------------------------------------------
  /**
   * SignatureRecord = Map[Signature, #occurrences]
   */
  type SignatureRecord = Map[String, Int];
  
  /**
   * SignatureRecordDecision = (Decision/Regression-value, Map[Signature, #occurrences]
   */
  type SignatureRecordDecision = (Double, Map[String, Int]);
  
  /**
   * Sig2ID_Mapping = (Signature, Signature ID), Where Signature ID is the unique ID that
   * corresponds to this unique signature (in this universe of Signatures, might exist several!)
   */
  type Sig2ID_Mapping = (String, Long);
  
  /**
   * SignatureRecord_ID = Map[SignatureID, #occurrences]
   * This corresponds to a molecule after Signature generation and make sense only in combination with 
   * the Sig2ID_Mapping that give the translation SignatureID->Signature
   */
  type SignatureRecord_ID = Map[Long, Int];
  
  /**
   * SignatureRecordDecision_ID = (Decision/Regression-value, Map[SignatureID, #occurrences])
   * This corresponds to a molecule after Signature generation and make sense only in combination with 
   * the Sig2ID_Mapping that give the translation SignatureID->Signature
   */
  type SignatureRecordDecision_ID = (Double, Map[Long, Int]);
  
}