#！bin/bash
#set -x
basepath=$(cd `dirname $0`; pwd)
cd $basepath/
mvn clean package -Dmaven.test.skip=true