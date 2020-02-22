#!/bin/bash

find . -type f -exec sed -i 's/\x0D$//' {} \;

find . -type d -exec chmod 777 {} \;
find . -type f -exec chmod 777 {} \;

find . -name '__pycache__' | xargs rm -rf
rm -f ./lib/pyfiles.zip
rm -rf ./.idea/
rm -rf ./.pytest_cache/

cd src/ && zip -r ../lib/pyfiles.zip com/

cd lib/jars
wget https://repo1.maven.org/maven2/com/databricks/spark-xml_2.11/0.8.0/spark-xml_2.11-0.8.0.jar
cd ../../
echo "Installation complete"
