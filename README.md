# Spark cheminformatics tools

This repository contains some utilities that can be used to build Spark-based massively parallel cheminformatics pipelines.

## Parsers

This package provides custom Spark input formats for chemical structures. In order to use it in you maven project please add this entries to your pom.xml file:

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

  
