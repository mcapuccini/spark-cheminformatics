# Spark cheminformatics utils

This repository contains some utilities that can be used to build Spark-based massively parallel cheminformatics pipelines.

## Parsers

This package provides custom Spark input formats for chemical structures. To use it in your maven project please add this entries to your pom.xml file:

    <repositories>
        ...
        <repository>
            <id>pele.farmbio.uu.se</id>
            <url>http://pele.farmbio.uu.se/artifactory/libs-snapshot</url>
        </repository>
        ...
    </repositories>

    <dependencies>
        ...
        <groupId>se.uu.farmbio</groupId>
            <artifactId>parsers</artifactId>
            <version>0.0.1-SNAPSHOT</version>
        </dependency>
        ...
    </dependencies>

### Usage 
This is how you read a SDF file using the SDFInputFormat (for other input formats works in the same way).

    val sc = new SparkContext(conf)
    //This sets the number of SDF loaded as a single string, in many cases it makes sense to 
    //have it set to more than 1 in order to reuse objects within a single mapper. Default
    //value is 30 for SDF.
    sc.hadoopConfiguration.set("se.uu.farmbio.parsers.SDFRecordReader.size", 30)
    val sdfRDD = sc.hadoopFile[LongWritable, Text, SDFInputFormat]("/path/to/molecules.sdf")

## Signature Generation

This library creates complete molecule-signatures (a type of molecular fingerprint) and provides functionality to create [LabeledPoint](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint) and [Vector](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.linalg.Vector)s which can be used for machine-learning using Spark. To use this library using Maven, add the following to your `pom.xml` file:
    
    <repositories>
        ...
        <repository>
            <id>pele.farmbio.uu.se</id>
            <url>http://pele.farmbio.uu.se/artifactory/libs-snapshot</url>
        </repository>
        ...
    </repositories>

    <dependencies>
        ...
        <groupId>se.uu.farmbio</groupId>
            <artifactId>sg</artifactId>
            <version>0.0.1-SNAPSHOT</version>
        </dependency>
        ...
    </dependencies>

The API include the following functions, where each function is provided in two flavours; either the straightforward way of computing directly, or by sending data together with your objects (i.e. if you wish to assign an identifier to each molecule/record). See the explination bellow. 

| Function name             | Description                    |
| -----------------------   | ------------------------------ |
|`atom2SigRecord`           | Performs Signature-generation on a single molecule and returns a `Map[String,Int]` of (Signature->#occurrences) |
|`atom2SigRecordDecision`   | Does the same thing as `atom2SigRecord`, but also requires a decision-class/regression-value for each molecule |
|`sig2ID`                   | Takes an RDD of Signature-Records and computes both the "universe of Signatures" and transform each record to use the Signature IDs instead |
|`sig2LP`                   | Transform all molecule-records that uses (Signature ID->#occurrences) into an RDD of [`LabeledPoint`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint)|
|`saveAsLibSVMFile`         | Allows callers to save `RDD[LabeledPoint]` as LibSVM-format (simply a wrapper of the same function in Sparks [`MLUtils`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) object)|
|`loadLibSVMFile`           | Allows callers to read a LibSVM-formatted file into an `RDD[LabeledPoint]`(simply a wrapper of the same function in Sparks [`MLUtils`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.util.MLUtils$) object)|
|`saveSign2IDMapping`       | Allows callers to save the "Signature universe" to file |
|`loadSign2IDMapping`       | Allows callers to load a "Signature universe" from file |
|`atom2LP`| Allows callers to compute a [`LabeledPoint`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint) from an `IAtomContainer` using an existing Signature Universe (i.e. useful for testing models with new data |
|`atoms2LP`| Allows callers to compute several `LabeledPoint` objects form an RDD of `IAtomContainers`, this method is made for creating test-data using an existing "Signature Universe" |
|`atoms2LP_UpdataSignMapCarryData`| This function allows the caller to compute [`LabeledPoint`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.mllib.regression.LabeledPoint) objects for several molecules at the same time, and simultaneously adding new signatures (that not previously has been seen) to the Signature Universe. This function can be used instead of using the standard fashion `atom2SigRecord`, `sig2ID` and `sig2LP` after each other (pass `null` as value for parameter `old_signMap`) |
|`atoms2Vectors`| Allows callers to compute `Vector`s using an existing "Signature Universe", to create testing data |

Note that all types defined can be found in the `uu.farmbio.sg.types`-object.

### Usage 
The default-way of using this API is the following: 
```scala
// Creating LabeledPoint-records for ML
val mols: RDD[IAtomContainer] = ... 
val moleculesAfterSG = mols.map{molecule => SGUtils.atom2SigRecordDecision(molecule, molecule.decision_class, h_start=1, h_stop=3)};
val (sig2ID_records, sig2ID_universe) = SGUtils.sig2ID(moleculesAfterSG);
val resultAsLP: RDD[LabeledPoint] = SGUtils.sig2LP(sig2ID_records);
...
// Creating Vector-records to predict on
val testMols: RDD[IAtomContainer] = ...
val test: RDD[Vector] = SGUtils.atoms2Vectors(testMols, sig2ID_universe, h_start=1, h_stop=3);
...
```
where `molecule` should be an `IAtomContainer` created by a reader from [`CDK`](https://github.com/cdk/cdk). The `resultAsLP` can then be used in ML-analysis in later steps, and the `sig2ID_universe` is the linking between a certain signature to the signature ID that is used in the LabeledPoint-records.  

#### Assigning data to your records
If you wish to assign some molecule ID or further data together wit your records, you can use the same _carryData-version of the API-function. Each of those functions will let you send data of type `RDD[(T, requiredType)]` where the requiredType is the type of the 'normal' API-function. The type `T` can be anything (following the Spark-requirement of it being serializable, either by standard Java Serialization or by Kryo, depending on your settings). If you wish to send more than one type of data with your record, simply put it into a tuple: `(name, year)` for instance. 
