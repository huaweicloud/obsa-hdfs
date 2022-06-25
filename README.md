### 概述
 ![image](https://user-images.githubusercontent.com/10207782/175764329-da0d34e0-cd4d-4a66-ae9a-1ab3c8060c2f.png)

1.ranger-obs-plugin：提供 Ranger 服务端的服务定义插件。它提供了Ranger侧的OBS服务权限控制；部署了该插件后，用户即可在 Ranger 的控制页面上，填写相应的权限策略。
2.ranger-obs-service：该服务周期性从 Ranger 服务端同步权限策略，并且在收到客户端的鉴权请求后在本地进行权限校验。 
3.ranger-obs-client：hadoop-obs对集成此插件，把需要权限校验的请求转发给ranger-obs-service。

### 源码编译 
1.进行ranger-obs目录
2.mvn clean package -Dmaven.test.skip=true
3.生成如下组件
ranger-obs/ranger-obs-client/target/ranger-obs-client-0.1.0.jar
ranger-obs/ranger-obs-plugin/target/ranger-obs-plugin-0.1.0.tar.gz
ranger-obs/ranger-obs-service/target/ranger-obs-service-0.1.0.tar.gz

### 安装部署指南
以下已hadoop安全集群为例，非安全集群去除kerberos相关操作即可。

#### 一.hadoop-obs安装
见https://support.huaweicloud.com/bestpractice-obs/obs_05_1507.html

#### 二.ranger-obs-plugin安装
（1）解压
解压ranger-obs-plugin-0.1.0.tar.gz，包含如下：
ranger-obs-plugin-0.1.0.jar：ranger obs插件包
ranger-obs.json：配置文件
（2）放入rangeradmin目录
将ranger-obs-plugin-0.1.0.jar放入/ranger-admin/ews/webapp/WEB-INF/classes/ranger-plugins/obs目录下
注意obs目录的用户和用户组以及ranger-obs-plugin-0.1.0-SNAPSHOT.jar的权限
（3）重启 Ranger 服务
（4）在 Ranger 上注册 OBS service
curl -v -k -urangeradmin:用户名@密码 -X POST -H "Accept:application/json" -H "Content-Type:application/json" -d @./ranger-obs.json https://ranger地址/service/plugins/definitions
（5）在下面的目录下创建obs service
![image](https://user-images.githubusercontent.com/10207782/175764704-449cc7ce-6f32-4a1f-906c-0777a24444e0.png)

![image](https://user-images.githubusercontent.com/10207782/175764578-0457eb75-701b-4cf6-b429-38d5197c8da6.png)

#### 三.ranger-obs-service安装
1.增加ranger-obs-service启动用户
（1）增加kerberos用户：
addprinc -randkey rangerobs/hadoop@NOVALOCAL
ktadd -k /etc/security/keytabs/rangerobs.keytab rangerobs/hadoop@NOVALOCAL
（2）增加对应的本地用户：useradd rangerobs -g hadoop -p rangerobs
2.解压ranger-obs-service-0.1.0.tar.gz，包含如下：
（1）bin：脚本目录
star_rpc_server.sh：华为云mrs集成使用，里面会涉及很多mrs特有的相关信息
start_server.sh：开源大数据集群使用
（2）conf：配置文件目录
core-site.xml和hdfs-site.xml：访问HDFS服务时需要的配置文件（此服务依赖HDFS服务）
ranger-obs-security.xml和ranger-obs-audit.xml：访问rangerAdmin服务的配置文件
ranger-obs.xml：ranger-obs-service服务自身的主配置文件
Kdc.conf和rangerobs.keytab等：其他可选配置文件
log4j.properties：日志配置文件
（3）lib：依赖包目录

3.配置：根据自身环境填写必选项即可，其他保持默认值
（1）core-site.xml和hdfs-site.xml配置文件：
可以从hadoop根目录/etc/hadoop/目录下拷贝过来
注意：ranger-obs-service的conf/core-site.xml和hdfs-site.xml配置文件中不要出现ranger.obs.xxx相关的配置项
（2）ranger-obs.xml配置文件：必填配置项
    <!-- 启动ranger-obs-service的kerberos用户 -->
    <property>
        <name>ranger.obs.service.kerberos.principal</name>
        <value>rangerobs/hadoop@NOVALOCAL</value>
    </property>
    <!-- 启动ranger-obs-service的kerberos用户对应的kt文件 -->
    <property>
        <name>ranger.obs.service.kerberos.keytab</name>
        <value>/etc/security/keytabs/rangerobs.keytab</value>
    </property>
（3）ranger-obs-security.xml配置文件：
    <!-- 从rangerAdmin服务拉取的policy存入本地缓存目录，会自动创建缓存目录 -->
    <property>
        <name>ranger.plugin.obs.policy.cache.dir</name>
        <value>xxx/ranger-obs-service-0.1.0/cache</value>
    </property>
    <!-- ranger admin的restful http地址，注意地址为主机名形式 -->
    <property>
        <name>ranger.plugin.obs.policy.rest.url</name>
        <value>http://rangerAdmin地址:6080</value>
    </property>
4.启动
（1）可选操作：编辑start_server.sh脚本，修改如下变量
native_path=hadoop根目/lib/native/
（2）启动：nohup ./start_server.sh &
注意：ranger-obs-service进程在启动时将访问HDFS的/user/目录，请赋权ranger-obs-service进程启动用户访问HDFS的/user/目录的权限

#### 四.ranger-obs-client安装
1.安装：
将ranger-obs-client-0.1.0-SNAPSHOT.jar和ranger-obs-client-0.1.0-SNAPSHOT-tests.jar放入hadoop-obs同目录下
2.配置
在hadoop组件目录下的core-site.xml文件中增加如下配置项：
ranger.obs.service.rpc.address：ranger-obs-service服务地址，支持配置多个地址，以分号分割
ranger.obs.service.kerberos.principal：rangerobs/hadoop@NOVALOCAL
fs.obs.authorize.provider：org.apache.hadoop.fs.obs.security.RangerAuthorizeProvider：当配置了此参数后hadoop-obs模块才会走ranger鉴权逻辑，否则将不走ranger鉴权逻辑

 
 
#### 用户手册
1.ranger-admin配置
（1）进入obs service
![image](https://user-images.githubusercontent.com/10207782/175765246-5d5b8f0e-bee0-4795-9a0a-24304319be34.png)

（2）创建 policy
![image](https://user-images.githubusercontent.com/10207782/175765252-b1a46799-ef30-4ec3-9320-c8daad92d121.png)

相关参数含义如下：
bucket：OBS桶名称
path：对象路径，支持通配符，注意 对象路径不以/开始。
include：表示设置的权限适用于 path 本身，还是除了 path 以外的其他路径。
recursive：表示权限不仅适用于 path，还适用于 path 路径下的子成员（即递归子成员）。通常用于 path 设置为目录的情况。
注意：对于桶根目录，因为对应的对象路径为空字符串，目前仅能通过*通配符进行设置，建议仅为管理员设置根目录的权限
user/group：用户名和用户组。这里是或的关系，即用户名或者用户组满足其中一个，即可拥有对应的操作权限。
Permissions：
Read：读操作。对应于对象存储里面的 GET、HEAD 类操作，包括下载对象、查询对象元数据等。
Write：写操作。对应于对象存储里面的 PUT 类等修改操作，例如上传对象。

2.HDFS命令测试
（1）通过kinit认证一个用户
（2）执行各种hadoop fs命令
