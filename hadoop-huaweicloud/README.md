示例：mvn clean install -Pdist -Dhadoop.plat.version=3.1.1 -Dhadoop.version=3.1.1 -Dmaven.test.skip=true

Pdist：将hadoop-obs依赖的obs java sdk以及okttp等依赖进行了shade，编译部署安装使用
Dhadoop.version：定义了依赖的hadoop版本，目前仅支持依赖hadoop-2.8.x及以上版本
Dhadoop.plat.version：定义了jar的命名规范，和-Dhadoop.version保持一致即可
jar包命名规范：hadoop-huaweicloud-x.x.x-hw-y.jar包含义：前三位x.x.x为依赖的hadoop版本；最后一位y为hadoop-obs版本，例如：hadoop-huaweicloud-3.1.1-hw-45.jar，3.1.1是配套的hadoop版本，45是hadoop-obs的版本
=========================================================================
Version 3.1.1.54.5
```
【特性】支持各个接口的耗时、操作类型、userName、endpoint等请求信息
【特性】支持getFileChecksum，即CRC32校验
【特性】支持setAttr、getAttr等与accesslabel属性配合使用文件目录属性设置和获取，
使用方式：
    fs.setXAttr(filePath, "AccessLabel", setValue);// 设置filePath目录的AccessLabel属性值
    fs.getXAttr(filePath, "AccessLabel");// 获取filePath目录的AccessLabel属性值
注意：setXAttr和getXAttr仅支持设置/获取AccessLabel属性值，其他属性值的设置和获取不支持
【优化】升级SDK版本至3.24.12，将SDK的使用版本从esdk-obs-java-optimise升级至esdk-obs-java-bundle
【优化】多段上传的大小最小由5M下放到1M，即现在支持多段的最小段大小为1M
【BUG】上传对象的缓存文件命名增加时间戳，修复缓存文件被覆盖数据不一致问题
```
=========================================================================

Version 3.1.1.54.3
 
【优化】修复53.8版本引入的创建文件夹后获取权限为rw-rw-rw-问题，恢复默认的为rwxrwxrwx
 
=========================================================================
 
Version 3.1.1.54.1
 
【优化】升级SDK版本至3.24.3.1
 
=========================================================================
 
Version 3.1.1.54.0/2.8.3.54.0

1. 【优化】支持append场景去除head行为，提升append的性能表现
2. 【注】该版本性能优化特性需要手动配置core-site.xml的配置项，新增如下配置
```txt
<property>
<name>fs.obs.appendRemoveHead</name>
<value>true</value>
</property>
```
=========================================================================

Version 3.1.1.53.8/2.8.3.53.8

1. 【优化】写数据流程加入sha256校验配置fs.obs.fast.upload.checksum.type，默认none
2. 【特性】支持hadoop distcp命令保留文件的属主(u)、属组(g)、权限(p)信息, 使用配置fs.obs.permissions.mode=disguise打开此特性

=========================================================================

Version 3.1.1.53.7/2.8.3.53.7

1. 【BUG】修复MemArtsCC场景下，Cache miss时，读统计数据放大的问题
2. 【优化】大数据回收站trashRoot目录路径优化，减少配置生命周期规则条数。使用配置fs.obs.hdfs.trash.version=2，默认路径前缀fs.obs.hdfs.trash.prefix=/user/.Trash

=========================================================================

Version 3.1.1.53.6.1/2.8.3.53.6.1
【优化】升级OBS SDK版本为3.23.3.1, 支持建链重试, 解决运满满依赖冲突

=========================================================================

Version 3.1.1.53.6/2.8.3.53.6
【优化】OBSA适配MemArtsCC SDK接口修改

=========================================================================

Version 3.1.1.53.5/2.8.3.53.5
【优化】在write和read中补充缺失的上报异常信息

=========================================================================

Version 3.1.1.53.4/2.8.3.53.4
【特性】实现WithErrCode接口 实现MRS+DLI+OBS客户侧故障感知能力

=========================================================================

Version 3.1.1.53.3/2.8.3.53.3
【BUG】修复MemArtsCCClient初始化失败仍然调用流量上报接口的Bug

=========================================================================

Version 3.1.1.53.1/2.8.3.53.1
【优化】新增对memartscc上报流量的开关

=========================================================================

Version 3.1.1.53/2.8.3.53
【特性】新增OBSA对memartscc上报流量统计的功能

=========================================================================

Version 3.1.1.52.2/2.8.3.52.2
【优化】memartscc跳过pyspark与spark引擎联动

=========================================================================

Version 3.1.1.52.1/2.8.3.52.1
【优化】新增memartscc跳过pyspark功能添加开关

=========================================================================

Version 3.1.1.52/2.8.3.52
【优化】增加写obs时disk缓存同步刷盘功能

=========================================================================

Version 3.1.1.51/2.8.3.51
【特性】支持obs select接口，试验阶段不可商用

=========================================================================

Version 3.1.1.50.9/2.8.3.50.9
【优化】写数据流程加入md5校验选项

=========================================================================

Version 3.1.1.50.8.1/2.8.3.50.8.1
【优化】新增MemArtsCC跳过pyspark开关

=========================================================================

Version 3.1.1.50.8/2.8.3.50.8
【优化】MemArtsCC Input Stream添加buffer pool

=========================================================================

Version 3.1.1.50.7/2.8.3.50.7
【BUG】修复OBSA ReadFully 偶现EOFException

=========================================================================

Version 3.1.1.50.6/2.8.3.50.6
【特性】支持 fs trash
【特性】去除Apache common lang 2.6依赖 使得OBSA支持 3.3.1
【特性】支持delegationToken机制
【BUG】override getHomeDirectory and fix mkdirs and fix putobject/appendobject retry

=========================================================================

Version 3.1.1.50.3/2.8.3.50.3
【优化】新metric监控打点框架修改点：所有接口在流控错误均上报监控打点

=========================================================================

Version 3.1.1.50.2/2.8.3.50.2
【优化】OBSA getContentSummary在服务端不支持的情况下，不重试，静默回退至v1版本
【优化】新metric监控打点框架

=========================================================================

Version 3.1.1.50.1/2.8.3.50.1
【特性】对接MemArtsCC特性（适配memartscc_3.22.4.T1版本）
【优化】将流控状态下的重试机制独立出来
【优化】优化range读取流程中多出1字节

=========================================================================

Version 3.1.1.50/2.8.3.50
【特性】对接MemArtsCC特性（适配memartscc_3.22.3.B001版本）
【特性】getContentSummary性能提升
【优化】升级obs sdk为3.22.3.1,支持getContentSummary性能提升
【优化】缩减元数据访问次数
【优化】代码可信整改
【优化】遇到503流控时展示详细流控状态码
【优化】为快速删除特性参数fs.obs.trash.dir添加默认值：FileSystem.getTrashRoot() + “FastDelete”
【优化】遇到400状态码时不重试
【优化】异常日志中添加详细错误码，利用问题快速定位
【优化】改变maven-shade-plugin插件行为，避免集成方对obs java sdk的重复引用
【BUG】seek超出文件末尾时抛出EOFException

=========================================================================

Version 3.1.1.46.1/2.8.3.46.1
【优化】inputStream seek 超出文件末尾时抛出EOFException

=========================================================================

Version 3.1.1.46/2.8.3.46
【BUG】“对象桶”场景下在服务端持续返回503的情况下rename操作依然显示成功
【优化】在flink或是flume等场景中将频繁调用append接口，此接口因输出流position判断不准确导致出现很多不必要的warn级别的日志
【优化】hadoop-obs 访问OBS的TCP建链超时由120s改为5s
【优化】添加aksk获取方式的INFO级别日志
【优化】升级obs sdk为3.21.8.2，解决XXE漏洞
【新增】新增fs.obs.outputstream.hflush.policy参数控制hflush和hsync方法的行为
【新增】ranger-obs，ranger控制OBS鉴权，目前仅支持MRS集群集成此特性

=========================================================================

Version 3.1.1.45/2.8.3.45

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

------

Example: mvn clean install -Pdist -Dhadoop.plat.version=3.1.1 -Dhadoop.version=3.1.1 -Dmaven.test.skip=true

Pdist: Shades the dependencies such as obs java sdk and okttp on which hadoop-obs depends for compilation, deployment, and installation.
Dhadoop.version: defines the dependent Hadoop version. Currently, only hadoop-2.8.x and later versions are supported.
Dhadoop.plat.version: defines the jar naming rule. The value must be the same as that of -Dhadoop.version.
The JAR package name format is hadoop-huaweicloud-x.x.x-hw-y.jar. The first three characters x.x.x indicate the dependent Hadoop version. The last y indicates the hadoop-obs version, for example, hadoop-huaweicloud-3.1.1-hw-45.jar. 3.1.1 indicates the matching Hadoop version, and 45 indicates the hadoop-obs version.

=========================================================================

Version 3.1.1.53.8/2.8.3.53.8/2.7.2.53.8

1. [Optimization] The SHA256 check configuration fs.obs.fast.upload.checksum.type is added to the data write process. The default value is none.
2. [Feature] The hadoop distcp command can retain the file owner (u), owner group (g), and permission (p). This feature is enabled by configuring fs.obs.permissions.mode=disguise.

=========================================================================

Version 3.1.1.53.7/2.8.3.53.7/2.7.2.53.7

1. [BUG] In the MemArtsCC scenario, the read statistics are magnified when the cache misses.
2. [Optimization] The trashRoot directory path in the big data recycle bin is optimized to reduce the number of lifecycle rules to be configured. Use the configuration fs.obs.hdfs.trash.version=2. The default path prefix is fs.obs.hdfs.trash.prefix=/user/.Trash.

=========================================================================

Version 3.1.1.53.6.1/2.8.3.53.6.1/2.7.2.53.6.1
[Optimization] The OBS SDK version is upgraded to 3.23.3.1, which supports link setup retry and resolves dependency conflicts when the system is full.

=========================================================================

Version 3.1.1.53.6/2.8.3.53.6/2.7.2.53.6
[Optimization] The OBSA adapts to the MemArtsCC SDK interface.

=========================================================================

Version 3.1.1.53.5/2.8.3.53.5/2.7.2.53.5
[Optimization] The missing exception information is added to the write and read fields.

=========================================================================

Version 3.1.1.53.4/2.8.3.53.4/2.7.2.53.4
[Feature] The WithErrCode interface is used to detect faults on the customer side of MRS, DLI, and OBS.

=========================================================================

Version 3.1.1.53.3/2.8.3.53.3/2.7.2.53.3
[BUG] Fix the bug that the traffic reporting interface is invoked after the MemArtsCCClient fails to be initialized.

=========================================================================

Version 3.1.1.53.1/2.8.3.53.1/2.7.2.53.1
[Optimization] The switch for reporting the traffic volume of the memartscc is added.

=========================================================================

Version 3.1.1.53/2.8.3.53/2.7.2.53
[Feature] The OBSA can collect statistics on the traffic reported by the memartscc.

=========================================================================

Version 3.1.1.52.2/2.8.3.52.2/2.7.2.52.2
[Optimization] The memartscc skips the association between the Pyspark and Spark engine.

=========================================================================

Version 3.1.1.52.1/2.8.3.52.1/2.7.2.52.1
[Optimization] The switch for skipping the pyspark function is added for the memartscc service.

=========================================================================

Version 3.1.1.52/2.8.3.52/2.7.2.52
[Optimization] The disk cache flushing function is added when data is written to OBS.

=========================================================================

Version 3.1.1.51/2.8.3.51/2.7.2.51
[Feature] The OBS select interface is supported. It cannot be put into commercial use in the trial phase.

=========================================================================

Version 3.1.1.50.9/2.8.3.50.9/2.7.2.50.9
[Optimization] The MD5 check option is added to the data write process.

=========================================================================

Version 3.1.1.50.8.1/2.8.3.50.8.1/2.7.2.50.8.1
[Optimization] The function of skipping pyspark for MemArtsCC is added.

=========================================================================

Version 3.1.1.50.8/2.8.3.50.8/2.7.2.50.8
[Optimization] The buffer pool is added to the MemArtsCC Input Stream.

=========================================================================

Version 3.1.1.50.7/2.8.3.50.7/2.7.2.50.7
[BUG] Occasionally EOFException Occurs in OBSA ReadFully

=========================================================================

Version 3.1.1.50.6/2.8.3.50.6/2.7.2.50.6
[Feature] fs trash
[Feature] The dependency on Apache common lang 2.6 is removed so that OBSA supports 3.3. 1.
[Feature] The delegationToken mechanism is supported.
[BUG] override getHomeDirectory and fix mkdirs and fix putobject/appendobject retry

=========================================================================

Version 3.1.1.50.3/2.8.3.50.3/2.7.2.50.3
[Optimization] In the new metric monitoring dotting framework, all interfaces report monitoring dotting when traffic control errors occur.

=========================================================================

Version 3.1.1.50.2/2.8.3.50.2/2.7.2.50.2
[Optimization] When OBSA getContentSummary is not supported by the server, the system does not retry and silently rolls back to V1.
[Optimization] New metric monitoring dotting framework

=========================================================================

Version 3.1.1.50.1/2.8.3.50.1/2.7.2.50.1
[Feature] Interconnection with MemArtsCC (memartscc_3.22.4.T1)
[Optimization] The retry mechanism in the flow control state is independent.
[Optimization] One extra byte is added in the range reading process.

=========================================================================

Version 3.1.1.50/2.8.3.50/2.7.2.50
[Feature] Interconnection with MemArtsCC (memartscc_3.22.3.B001)
[Feature] The getContentSummary performance is improved.
[Optimization] The OBS SDK is upgraded to 3.22.3.1 to improve the getContentSummary performance.
[Optimization] Reduce the number of metadata access times.
[Optimization] Code trustworthiness rectification
[Optimization] The detailed flow control status code is displayed when the 503 flow control occurs.
[Optimization] The default value FileSystem.getTrashRoot() + FastDelete is added for the fs.obs.trash.dir parameter.
[Optimization] No retry is performed when status code 400 is received.
[Optimization] Detailed error codes are added to exception logs to quickly locate faults.
[Optimization] The behavior of the maven-shade-plugin plug-in is changed to prevent the integrator from repeatedly referencing the obs java SDK.
[BUG] EOFException is thrown when the seek exceeds the end of the file.

=========================================================================

Version 3.1.1.46.1/2.8.3.46.1/2.7.2.46.1
[Optimization] When inputStream seek exceeds the end of the file, EOFException is thrown.

=========================================================================

Version 3.1.1.46/2.8.3.46/2.7.2.46
[BUG] In the object bucket scenario, the rename operation is still successful even if the server continuously returns 503.
[Optimization] In scenarios such as Flink and Flume, the append interface is frequently invoked. This interface incorrectly determines the position of the output stream. As a result, many unnecessary warn-level logs are generated.
[Optimization] The TCP connection setup timeout for hadoop-obs to access OBS is changed from 120s to 5s.
[Optimization] The INFO-level log for obtaining the AK/SK is added.
[Optimization] The OBS SDK is upgraded to 3.21.8.2.
