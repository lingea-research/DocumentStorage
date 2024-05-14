#!/bin/bash

# -----------------------------------------------------------------------------
#
# Document Storage build script
#
# Script for compiling the document storage using maven. The script should also 
# move the compiled .jar somewhere where the indexingService expects it.
#
# -----------------------------------------------------------------------------

# Compile the DocumentStorage.

echo "Running: mvn clean install"
mvn clean install