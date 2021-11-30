
### flink-obs 使用指南
见华为云OBS服务官方文档：https://support.huaweicloud.com/bestpractice-obs/obs_05_1516.html

----------------------------------------

### flink-obs 源码编译指南
示例：mvn clean install  -Dmaven.test.skip=true -Dhadoop.version=3.1.1
-  flink-obs依赖hadoop-obs模块：请先将hadoop-obs模块安装到本地
- -Dhadoop.version：定义依赖的hadoop版本
- jar包命名规范：flink-obs-fs-hadoop-x.x.x-hw-y.jar版本规则：x.x.x为对应的flink版本，y为flink-ob版本，例如：flink-obs-fs-hadoop-1.12.1-hw-45.jar，1.12.1是对应的flink版本，45是flink-obs的版本
-  flink-obs的实现基于flink的plugin加载机制（flink从1.9开始引入），flink-obs必须通过flink的plugin机制进行加载，即将flink-obs放入${FLINK_HOME}/plugins/obs-fs-hadoop目录下
----------------------------------------

### flink-obs release
Version 1.12.1.45
【功能】新增flink抽象文件系统的OBS实现，以支持flink存放state，sink等数据到OBS的场景
