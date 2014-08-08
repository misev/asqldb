#!/bin/bash
#
# mavenHandler.sh - create or deploy package from asqldb
# 
# SYNTAX
# 		mavenHandler.sh [clean] [package] [deploy]
# 
# DESCRIPTION 
# 		This script creates, deploys the package to the remote repository specified in the POM. 
# 		Before running mvn package or mvn deploy the targets are pre-cleaned.
# 		The clean arguments prompts the script to delete everything created in the process of packaging or deploying. 
# 	
# PRECONDITIONS
# 		Maven and gpg must be installed for successfull packaging and deploying.
# 
		
# --- constants
PROG=`basename $0`
ARG=$(echo "$1" | awk '{print tolower($0)}')


# script return codes
RC_OK=0

# path and folder variables
BASE_FOLDER=mvnasqldb
MAVEN_SOURCE_STRUCT=$BASE_FOLDER/src/main/java
PACKAGE_PATH=org/hsqldb
SOURCE_BASE=../../src/*
TEST_FOLDER=test

#routines
clean(){
	echo "Prog: Removing $BASE_FOLDER"
	rm -rf $BASE_FOLDER
}

setup(){
	echo "$PROG: Create basefolder $BASE_FOLDER"
	mkdir -p $BASE_FOLDER

	echo "$PROG: Create maven archetype"
	mkdir -p $MAVEN_SOURCE_STRUCT

	echo "$PROG: Copying POM to the $BASE_FOLDER"
	cp pom.xml $BASE_FOLDER

	echo "$PROG: Copying sources to $MAVEN_SOURCE_STRUCT from $SOURCE_BASE"
	cp -r $SOURCE_BASE $MAVEN_SOURCE_STRUCT

	echo "$PROG: Cleaning up tests from the artifact"
	rm -rf $MAVEN_SOURCE_STRUCT/$PACKAGE_PATH/$TEST_FOLDER	
}

if [ "$ARG" == "deploy" ]
then
	setup
	cd $BASE_FOLDER && mvn clean deploy
elif [[ "$ARG" == "clean" ]]; then
	clean
elif [[ "$ARG" == "package" ]]; then
	setup
	cd $BASE_FOLDER && mvn clean package
else
	echo "Usage: $PROG [deploy] [clean] [package]"
fi

exit $RC_OK



