# Compiliation guide

This project is origanized by Maven, you can build this project like the following examples:

## hadoop-huaweicloud
```
cd hadoop-huaweicloud
mvn clean package -Dmaven.test.skip=true -Dhadoop.plat.version=3.1.1 -Dhadoop.version=3.1.1 -P dist
```
