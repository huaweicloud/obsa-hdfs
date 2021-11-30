# Compiliation guide

This project is origanized by Maven, you can build this project like the following examples:

## hadoop-huaweicloud
```
cd hadoop-huaweicloud
mvn clean package -Dmaven.test.skip=true -Dhadoop.plat.version=3.1.1 -Dhadoop.version=3.1.1 -P dist
```
### 概述
此仓库包含了两个项目
- hadoop-obs项目：基于华为云OBS服务实现了hadoop文件系统抽象，位于master分支hadoop-huaweicloud目录，见对应的readme.md
- flink-obs项目：基于华为云OBS服务实现了Flink文件系统抽象，位于flink-obs分支flink-obs-fs-hadoop目录，见对应的readme.md
