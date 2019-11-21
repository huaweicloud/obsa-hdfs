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
