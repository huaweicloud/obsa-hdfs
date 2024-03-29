
Version 3.1.1.45/2.8.3.45/2.7.2.45

修复问题：
1. 【功能】新增预读策略，优化顺序读性能
2. 【优化】修复文件桶追加输出流的getPos语义的正确性
3. 【优化】将文件桶truncate接口中的HadoopIllegalArgumentException异常封装为IOException
4. 【优化】快速删除trash目标对象存在时，添加时间戳后缀到ms级，并按最大时间做冲突重试
5. 【优化】文件桶递归删除目录409冲突时按最大时间重试
6. 【优化】create接口添加文件立即可见性开关，默认关闭
7. 【优化】去掉HDFSWrapper中的默认shema必须为hdfs的检查
8. 【优化】升级obs sdk版本为3.21.4.1，修复列举操作时最后文件的lastmodifytime字段不正确
9. 【优化】修改HDFSWrapper中的rename抛出异常为IOException

=========================================================================

Version 3.1.1.43/2.8.3.43/2.7.2.43

修复问题：
1. 【功能】新增OBSWrapper特性，用于支持已hdfs schema方式对接OBS
2. 【功能】针对对象桶修复目录过大rename性能问题
3. 【功能】修复快速删除功能trash时并发冲突问题
4. 【用例】补充OBSWrapper功能用例
5. 【用例】补充trash并发冲突场景用例

=========================================================================

Version 3.1.1.42/2.8.3.42/2.7.2.42

修复问题：
1. 【功能】解决本地写缓存文件名字符串，进程退出才清理导致的OOM问题
2. 【功能】替换依赖的hadoop-common2.8.3为修复安全漏洞的2.8.3.0101-hw-ei-14
3. 【功能】Metrics统计删除冗余的参数，并提供close接口用于退出时释放资源
4. 【功能】排除hadoop-common中对jetty非安全包的依赖

=========================================================================

Version 3.1.1.41/2.8.3.41/2.7.2.41

修复问题：
1. 【功能】支持public接口的metrics统计
2. 【功能】调用create()接口后，文件立即可见
3. 【功能】支持FileSystem/InputStream/OutputStream关闭后拒绝接口访问
4. 【功能】finalize()接口不再调用带I/O操作的close()，以防止上层误用
5. 【功能】pom文件依赖排除hadoop-common 3.1.1引入的jackson-databind不合规版本
6. 【功能】实现getCanonicalServiceName接口，用于提升HBase BulkLoad场景同一桶内加载性能
7. 【用例】补充完善重试机制测试用例

=========================================================================

Version 3.1.1.40/2.8.3.40/2.7.2.40

修复问题：
1. 【功能】接口语义排查，按原生HDFS语义实现
2. 【功能】开源社区规范整改
3. 【功能】truncate功能支持
4. 【功能】目录名不带/细粒度授权policy支持
5. 【功能】递归列举接口未更新目录lasymodifyTime问题修复
6. 【功能】重试机制增强，最大支持180s时间重试
7. 【功能】初始化支持探测写缓存的可访问性，开关控制，默认关闭
8. 【功能】OBS JAVA SDK调整为3.20.6.1版本

=========================================================================

Version 3.1.1.39/2.8.3.39/2.7.2.39

修复问题：
1. 【功能】静态检查和安全扫描整改
2. 【功能】接口测试用例补充


=========================================================================


Version 3.1.1.38/2.8.3.38/2.7.2.38

修复问题：
1. 【功能】修改listStatus接口返回的目录的lastModifyTime为准确值


=========================================================================


Version 3.1.1.37/2.8.3.37/2.7.2.37

修复问题：
1. 【功能】seekInStream方法中skip改为while循环，及去掉available判断
2. 【功能】ObsClient及socket buffer大小改为256KB
3. 【功能】OBS SDK 连接池优化开关可配置
4. 【功能】OBSBlockOutputStream日志打印优化


=========================================================================

Version 3.1.1.36/2.8.3.36/2.7.2.36

修复问题：
1. 【功能】eSDK鉴权协商增加开关，默认关闭
2. 【功能】初始化时head bucket增加异常重试逻辑
3. 【功能】onReadFailure中reopen异常时增加重试逻辑
4. 【功能】append stream在客户端记录文件长度，修复可能的异常
5. 【功能】增加递归listStatus接口，提升大目录列举性能
6. 【功能】trash对象时，如果trash目录下存在同名对象，新对象时间戳后缀不带冒号
7. 【用例】
   （1）增加用例验证eSDK鉴权协商开关有效性
   （2）增加head bucket重试机制测试用例
   （3）增加onReadFailure中reopen重试机制测试用例
   （4）增加head文件长度无效时，append stream用例
   （5）增加listStatus递归列举用例
   （6）增加trash文件和trash目录的测试用例


=========================================================================

Version 3.1.1.35/2.8.3.35/2.7.2.35

修复问题：
1. 【功能】OBSFileInputStream.java中文件桶的rename()接口新增重试逻辑


=========================================================================

Version 3.1.1.33/2.8.3.33/2.7.2.33

修复问题：
1. 【功能】OBSBlockOutputStream finalize方法中关闭流，清理写缓存文件
2. 【功能】解决listFiles接口递归列举时遇到子目录抛空指针异常问题
3. 【功能】合入eSDK3.19.11.1版本，解决EcsObsCredentialsProvider偶现请求失败问题
4. 【用例】
   （1）增加listFiles接口测试用例


=========================================================================

Version 3.1.1.32/2.8.3.32/2.7.2.32

修复问题：
1. 【功能】list超时客户端并发列举优化
2. 【功能】read(ByteBuffer buffer)接口重载
3. 【功能】对象桶rename接口部分场景未按HDFS原生语义实现修复
4. 【功能】OBSFileInputStream.java中四个参数配置开关，默认走父类方法
5. 【功能】fs.obs.read.buff.size和fs.obs.write.buff.size默认值调整为64KB
6. 【功能】readaheadInputStreamEnabled预读相关代码注释
7. 【用例】
   （1）对象桶rename增加四个用例
   （2）增加read(ByteBuffer buffer)接口测试用例
   （3）增加四个参数read接口开关测试用例


=========================================================================

Version 3.1.1.31/2.8.3.31/2.7.2.31

修复问题：
1. 【功能】OBSFileInputStream.java中带四个参数read接口未按HDFS语义实现及返回长度bug修复
2. 【功能】list和head接口返回对象最后修改时间不一致问题修复
3. 【功能】Rename接口源和目的相同时，返回值未按原生HDFS语义实现问题修复
4. 【功能】递归删除BUG解决
5. 【用例】
   （1）完善getContentSummary用例，增加对递归删除的校验
   （2）增加四个参数read接口对不同入参处理和返回值判断用例
   （3）增加list和head对应用返回对象最后修改时间比较用例
   （4）增加rename接口源和目的相同时的用例


=========================================================================

Version 3.1.1.30/2.8.3.30/2.7.2.30

修复问题：
1. 【功能】OBSFileSystem.java中接口getFileStatus、copyFile、listObjects、continueListObjects增加异常重试逻辑
2. 【功能】OBSFileSystem.java中renameFolder中删除源目录实现由异步删除为批删
3. 【功能】OBSInputStream.java中read相关的3个接口增加异常重试逻辑
4. 【功能】对象桶getContentSummary性能优化
5. 【功能】支持uper jar包编译
6. 【用例】
   （1）增加getContentSummary用例
   （2）增加重试接口mock用例


=========================================================================

Version 3.1.1.29/2.8.3.27/2.7.2.29

修复问题：
1. 【功能】修改fs.obs.readahead.range默认值为1MB；
2. 【功能】当fs.obs.fast.upload.buffer为array时，申请的第一个block的大小默认修改为1MB，加了参数fs.obs.fast.upload.array.first.buffer;
3. 【功能】OBSBlockOutputStream增加多段上传过程中异常快速抛出，异常后，write、close都抛异常;
4. 【功能】OBSInputStream在seekInStream方法中，incrementBytesRead修改读取数据的值为真实skipped的字节数;
5. 【功能】fs.obs.connection.establish.timeout和fs.obs.connection.timeout默认值修改为120S
6. 【功能】修改deleteObejcts()接口，批删失败转单点删除，单点删除失败重试3次；
7. 【功能】修改批删触发条件，当开启enableMultiObjectsDelete开关，并且删除对象数大于等于3时才走批删
8. 【用例】
   （1）增加批量删除用例一个，覆盖批量请求目标对象数小于100、1000~2000、大于2000场景
   （2）增加array的block用例1个和putPart快速失败用例1个，覆盖array的第一个block size为默认1MB，以及覆盖putPart快速失败用例。
   （3）增加readahead默认值为1MB用例1个


=========================================================================

Version 3.1.1.28/2.8.3.28/2.7.2.28

修复问题：
1. 【功能】copyFromLocalFile对目录支持，直接调用super的copyFromLocalFile方法。
3. 【用例】增加copyFromLocalFile目录的用例

=========================================================================

Version 3.1.1.27/2.8.3.27/2.7.2.27

修复问题：
1. 【功能】增加匿名访问方式的obsclient初始化。
2. 【功能】将23版本的KMS和26版本的append stream只走posix桶的append方法合并。
3. 【用例】（1）增加MainTest方法，后面流水线就通过执行该方法完成跑自动化用例。
          （2）增加assembly.xml，在编译test时，需要使用assemly方式将test打包。
          打包方法：“compile  assembly:single -Ddescriptor=E:\bigdata\obs_bigdataonobs\hadoop-tools\hadoop-huaweicloud\src\test\resources\assembly.xml --settings C:\Users\user\apache-maven-3.5.4\conf\settings_cloudmonitor.xml”
          跑用例：java -cp hadoop-huaweicloud-2.8.3.26-assembly.jar org.apache.hadoop.fs.obs.MainTest

=========================================================================

Version 2.8.3.26/2.7.2.26 补丁版本

修复问题：
1. 【功能】针对posix桶，在rename被使用为move场景，需要加上move的key。修改方式为：依照S3方法，先做HEAD，再调rename接口。

=========================================================================

Version 2.8.3.25/2.7.2.25 补丁版本

修复问题：
1. 【功能】修改obsblockoutputstream的append模式，在大于阈值时，直接调用appendFS接口，不再走多段上传；
2. 【用例】新增append和hflush场景的4个用例：
       (1). hflush大对象（20MB）
       (2). hflush中对象（5MB）
       (3). hflush小对象（2MB）
       (4). appendstream根据缓存大小做了4次append操作；
3. 【优化】优化DELETE在409情况下，3次重试机制；
4. 【遗留问题】修改obsclient初始化方式，在credential provider\aksk\security provider都没有设置时，使用匿名方式初始化obsclient。

=========================================================================

Version 2.8.3.23/2.7.2.23

新增需求：
1. 【功能】新增参数对象加密功能，新增需参数：
（1）fs.obs.server-side-encryption-type：加密算法，参数值为：sse-kms or sse-c；
（2）fs.obs.server-side-encryption-key：当参数（1）的值为sse-kms时，该值可选，表示kms加密key id；当参数（1）为sse-c时，该值必选，表示base64 encoded content。
（3）kms加密需要将fs.obs.connection.ssl.enabled设为true，走https加密方式。

修复问题：
1. 【优化】 delete 方法加了在409的时候3次最大重试功能，减小外层任务失败的概率。

=========================================================================
Version 2.8.3.22/2.7.2.22

修复问题：
1. 【功能】优化Posix桶时的性能，减少元数据HEAD次数；

新增需求：
1. 【功能】新增参数fs.obs.security.provider，配合ESDK从环境变量和ECS获取AK SK信息。

=========================================================================
Version 2.8.3.20/2.7.2.20

修复问题：
1. 【功能】OBSFileSystem被GC，导致的OBSInputStream中obsclient的引用为空，引起空指针异常；

=========================================================================
Version 2.8.3.19/2.7.2.19

修复问题：
1. 【功能】HBASE场景，未关闭JAVA SDK的连接，导致EOF异常；
