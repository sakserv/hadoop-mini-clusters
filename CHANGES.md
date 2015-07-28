Change Log
==========
### 0.0.14 - 07/28/2015
* Added Oozie Support
* Added optional argument to HDFS to enable the current user as a proxy user

### 0.0.13 - 07/04/2015
* Added YARN Support
* Added MRv2 Support
* Added HBase Support
* Added support for the InJvmContainerExecutor
* Updated dependencies to apache releases due to HWX repo issues
* Added additional details to the README
* 98% code coverage for all classes (less InJvmContainerExecutor)

### 0.0.12 - 02/08/2015
* Added HyperSQL support

### 0.0.11 - 02/02/2015
* Fixed shade plugin to resolve corrupt jar issues
* Added usage examples to README

### 0.0.10 - 02/02/2015 - DO NOT USE
* RELEASE NUKED DUE TO SHADE PLUGIN PRODUCING A BAD JAR
* Breaking Change: Moved all mini clusters to the builder pattern
* Moved configuration to properties file
* Split unit and integration tests
* Refactored the pom

### 0.0.9 - 01/19/2015 - DO NOT USE
* RELEASE NUKED DUE TO SHADE PLUGIN PRODUCING A BAD JAR
* Moved to log4j
* Added proper assertions
* Option to wait on topology kill for StormLocalCluster
* Added ASL headers
* Added a proper README

### 0.0.8 - 01/08/2015
* Added embedded MongodbLocalServer

### 0.0.7 - 01/07/2015
* Added missing calls to cleanUp()

### 0.0.6 - 01/06/2015
* First Release
