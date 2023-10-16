### 概述
 ![image](https://user-images.githubusercontent.com/10207782/175764329-da0d34e0-cd4d-4a66-ae9a-1ab3c8060c2f.png)

1.ranger-obs-plugin：该组件为ranger admin提供华为云OBS服务插件，它提供了OBS服务权限控制；部署了该插件后，用户即可在 Ranger admin中填写相应的权限策略；

2.ranger-obs-service：该组件提供RPC服务，其周期性的拉取ranger admin中OBS服务相关的权限策略，且在收到hadoop-obs/ranger-obs-client的鉴权请求后在本地进行权限校验；此服务是可以横向扩展的，即您可以部署启动多个ranger-obs-service服务用于负载均衡，其前端可以部署nginx服务提供统一的服务入口

3.ranger-obs-client：该组件通常和hadoop-obs组件集成在一起，把需要权限校验的请求转发给ranger-obs-service；

### 源码编译 
1.进入ranger-obs目录

2.mvn clean package -Dmaven.test.skip=true  -Dhadoop.version=3.1.1 -Dranger.version=2.0.0 -Dhadoop.huaweicloud.version=3.1.1-hw-46.1

-Dhadoop.version：定义了依赖的hadoop版本

-Dranger.version：定义了依赖的ranger版本

-Dhadoop.huaweicloud.version：定义了hadoop-obs组件的版本

附：如何从maven仓库获取hadoop-obs等组件

在ranger-obs/pom.xml中添加如下maven仓库地址

    <repositories>
        <repository>
            <id>HuaweiCloudSDK</id>
            <url>https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/</url>
            <releases>
                <enabled>true</enabled>
            </releases>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
        </repository>
    </repositories>
3.生成如下组件

ranger-obs/ranger-obs-client/target/ranger-obs-client-0.1.0.jar

ranger-obs/ranger-obs-plugin/target/ranger-obs-plugin-0.1.0.tar.gz

ranger-obs/ranger-obs-service/target/ranger-obs-service-0.1.0.tar.gz


### 安装部署指南
--我们以非安全集群为例，其支持安全集群，安全集群需要进行kerberos相关配置
--我们以hadoop-3.1.1，hadoop-huaweicloud-3.1.1-hw-46.1.jar，ranger2.0.0为例，其他版本并未经过测试，请您自行进行兼容性测试

#### 一.hadoop和hadoop-obs安装配置
1.下载并安装hadoop-3.1.1到/home/modules/hadoop-3.1.1/

2.下载hadoop-obs：
https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/com/huaweicloud/obs/hadoop-huaweicloud/3.1.1-hw-46.1/

3.安装配置hadoop-obs：
https://support.huaweicloud.com/bestpractice-obs/obs_05_1507.html

#### 二.ranger-admin和ranger-usersync安装

1.下载ranger2.0.0源码进行编译

（1）下载：http://archive.apache.org/dist/ranger/2.0.0/apache-ranger-2.0.0.tar.gz

（2）编译：mvn -Pall -DskipTests=false clean compile package install

在编译后的target目录中获取相关tar.gz

2.安装ranger-admin

（1）请提前安装mysql，python

（2）解压ranger-2.0.0-admin.tar.gz到/home/modules/ranger/ranger-2.0.0-admin

（3）配置ranger-2.0.0-admin/install.properties文件：如下设置供参考，可依据您的自身要求进行相关配置项的调整

<details> 
    <summary>展开查看</summary>
    <pre><code>
DB_FLAVOR=MYSQL
#请自行下载mysql-connector-java-5.1.47.jar
SQL_CONNECTOR_JAR=/home/modules/ranger/ranger-2.0.0-admin/mysql-connector-java-5.1.47.jar
#连接mysql数据库的用户名和密码和地址
db_root_user=xxx
db_root_password=xxx
db_host=xxx

#rangeradmin需要创建的数据库的名字，对应的用户名和密码
db_name=rangeradmin
db_user=rangeradmin
db_password=xxxxxxx

#rangerAdmin组件的密码
rangerAdmin_password=xxx
#Tagsync组件的密码
rangerTagsync_password=xxx
#Usersync组件的密码
rangerUsersync_password=xxx
#和上述保持一致
keyadmin_password=xxx

#注释掉audit_store
#audit_store=solr
     
 #rangeradmin的http地址
policymgr_external_url=http://node1.novalocal:6080
     
#rangeradmin进程启动的用户和用户组
unix_user=rangeradmin
unix_user_pwd=rangeradmin
unix_group=ranger
     
#rangeradmin组件的Kerberos认证相关配置，以下配置仅为示例，非安全集群请设置为空
spnego_principal=HTTP/ecs-bigdata-obs.novalocal@NOVALOCAL
spnego_keytab=/etc/security/keytabs/spnego.service.keytab
token_valid=30
cookie_domain=ecs-bigdata-obs.novalocal
cookie_path=/
admin_principal=rangeradmin/ecs-bigdata-obs.novalocal@NOVALOCAL
admin_keytab=/etc/security/keytabs/rangeradmin.keytab
lookup_principal=rangerlookup/ecs-bigdata-obs.novalocal@NOVALOCAL
lookup_keytab=/etc/security/keytabs/rangerlookup.keytab
hadoop_conf=/home/modules/hadoop-3.1.1/etc/hadoop
    </code></pre> 
</details>

（4）安装ranger-2.0.0-admin/setup.sh

（5）启动ranger-admin start
相关日志存放在ranger-2.0.0-admin/ews/logs目录下

（6）验证：浏览器打开http://{rangeradmin}:6080/
登陆信息：

用户名：admin

密码：为ranger-2.0.0-admin/install.properties配置文件中配置的rangerAdmin_password=xxx

3.安装ranger-usersync(可选)

（1）解压ranger-2.0.0-usersync.tar.gz到/home/modules/ranger/ranger-2.0.0-usersync

（2）配置ranger-2.0.0-usersync/install.properties文件：如下设置供参考，可依据您的自身要求进行相关配置项的调整

<details> 
    <summary>展开查看</summary>
    <pre><code>
#rangeradmin的url
POLICY_MGR_URL =http://{rangeradmin}:6080
 
#从unix操作系统同步用户信息
SYNC_SOURCE = unix
#同步间隔时间，单位(分钟)
SYNC_INTERVAL = 1

#运行此进程的linux用户
unix_user=rangerusersync
unix_group=ranger

#rangerUserSync用户的密码，参考ranger-admin中install.properties的配置
rangerUsersync_password=xxx

#Kerberos相关配置，以下配置仅为示例，非安全集群请设置为空
usersync_principal=rangerusersync/ecs-bigdata-obs.novalocal@NOVALOCAL
usersync_keytab=/etc/security/keytabs/rangerusersync.keytab
hadoop_conf=/home/modules/hadoop-3.1.1/etc/hadoop
    </code></pre> 
</details>

（3）安装ranger-2.0.0-usersync/setup.sh

（4）配置/etc/ranger/usersync/conf/ranger-ugsync-site.xml

<property>
    <name>ranger.usersync.enabled</name>
    <value>true</value>
</property>

（5）启动ranger-usersync start
日志文件位于ranger-2.0.0-usersync/logs目录下

#### 三.ranger-obs-plugin安装
1.解压ranger-obs-plugin-0.1.0.tar.gz到/home/modules/ranger/ranger-obs-plugin-0.1.0

2.将jar包放入ranger-2.0.0-admin安装目录
在ranger-2.0.0-admin/ews/webapp/WEB-INF/classes/ranger-plugins/目录下创建obs目录

将ranger-obs-plugin-0.1.0.jar放入ranger-2.0.0-admin/ews/webapp/WEB-INF/classes/ranger-plugins/obs目录下

注意：保证obs目录和其他插件目录的属主和权限一致

3.重启ranger-admin服务

ranger-admin restart

4.在ranger-admin上注册 OBS service
cd ranger-obs-plugin-0.1.0

curl -v -k -u{user}:{password} -X POST -H "Accept:application/json" -H "Content-Type:application/json" -d @./ranger-obs.json http://{rangeradmin}:6080/service/plugins/definitions

user和password即为登陆rangeradmin界面的user和password

5.在如下界面创建obs service

![image](https://user-images.githubusercontent.com/10207782/175764704-449cc7ce-6f32-4a1f-906c-0777a24444e0.png)

![image](https://user-images.githubusercontent.com/10207782/175764578-0457eb75-701b-4cf6-b429-38d5197c8da6.png)

#### 四.ranger-obs-service安装
1.解压ranger-obs-service-0.1.0.tar.gz到/home/modules/ranger/ranger-obs-service-0.1.0

（1）bin：脚本目录

star_rpc_server.sh：华为云mrs服务集成时使用，脚本涉及了mrs服务特有的相关信息

start_server.sh：开源大数据集群使用

（2）conf：配置文件目录

core-site.xml和hdfs-site.xml：访问HDFS服务时需要的配置文件（此服务依赖HDFS服务）

ranger-obs-security.xml和ranger-obs-audit.xml：访问rangerAdmin服务的配置文件

ranger-obs.xml：ranger-obs-service服务自身的主配置文件

log4j.properties：日志配置文件

（3） lib：依赖的jar包目录

2.配置

（1）同步hadoop-3.1.1中的core-site.xml和hdfs-site.xml到ranger-obs-service-0.1.0/conf

（2）ranger-obs.xml
|  参数   | 是否必填  | 默认值  | 解释  |
|  ----  | ----  | ----  | ----  |
| ranger.obs.service.rpc.address | 否 | 0.0.0.0:26901 | ranger-obs-service服务RPC监听地址 |
| ranger.obs.service.status.port | 否 | 26900 | ranger-obs-service服务http监听端口 |
| ranger.obs.service.authorize.enable  | 否 | true |是否开启权限拦截，当为FALSE时所有权限检查放通 |
| ranger.obs.service.sts.enable  | 否 | false | 是否开启STS服务，此功能为实验功能 |
| ranger.obs.service.sts.provider | 否 | 空 | aksk或临时aksk提供器，提供器需实现org.apache.ranger.obs.security.sts.STSProvider接口 |
| ranger.obs.service.kerberos.principal  | 是（kerberos） | 无 | 安全集群配置项，ranger-obs-service服务运行用户 |
| ranger.obs.service.kerberos.keytab  | 是（kerberos） | 无 | 安全集群配置项，ranger-obs-service服务运行用户对应的keytab |
| ranger.obs.service.dt.service.name  | 否 | 0.0.0.0:26901 | 安全集群配置项，DelegationToken中的service name, 因为ranger-obs-service服务是可横向扩展的，所以所有ranger-obs-service的此配置项务必保持一致 |
| ranger.obs.service.dt.manager  | 否 | org.apache.ranger.obs.security.token.SimpleSecretManager | 安全集群配置项，SimpleSecretManager以不存储token的方式进行DelegationToken管理 |
| ranger.obs.service.dt.secret.provider  | 否| org.apache.ranger.obs.security.token.ShareFileSecretProvider | 安全集群配置项，SimpleSecretManager的秘钥提供器 |
| ranger.obs.service.dt.secret.file  | 否 | 空 | 安全集群配置项，ShareFileSecretProvider秘钥提供器加载的秘钥文件在hdfs中的存储路径，如果没有配置秘钥文件位置默认自动生成并存储在hdfs://xxx/${user}/secret.jceks中 |
| ranger.obs.service.dt.renew-interval  | 否 | 86400000 | 安全集群配置项，DelegationToken 续租间隔, 单位ms， 默认86400000(1天) |
| ranger.obs.service.dt.max-lifetime  | 否 | 604800000 | 安全集群配置项，DelegationToken 续租间隔, 单位ms， 默认604800000(7天) |
| ranger.plugin.obs.service.name  | 否| obs | 务必对应rangerAdmin中的obs插件的service name |
| ranger.obs.service.rpc.handler.count  | 否| 10 | The number of RPC server handler threads |

（3）ranger-obs-security.xml
|  参数   | 是否必填  | 默认值  | 解释  |
|  ----  | ----  | ----  | ----  |
| ranger.plugin.obs.policy.rest.url  | 是 | 空 | 	rangerAdmin的url |
| ranger.plugin.obs.policy.cache.dir  | 是 | 空 | 从rangerAdmin服务拉取的policy存入本地缓存目录，会自动创建缓存目录 |
| ranger.plugin.obs.policy.source.impl  | 否 | org.apache.ranger.admin.client.RangerAdminRESTClient |  |
| ranger.plugin.obs.policy.pollIntervalMs  | 否 | policy拉取间隔 | 30000 |
| ranger.plugin.obs.policy.rest.client.connection.timeoutMs  | 120000 | 访问rangerAdmin的连接超时 |
| ranger.plugin.obs.policy.rest.client.read.timeoutMs  | 否 | 30000 | 访问rangerAdmin的读取超时 |
| ranger.plugin.obs.service.name  | 否 | obs | 务必对应rangerAdmin中的obs插件的service name |


（4）ranger-obs-audit.xml
|  参数   | 是否必填  | 默认值  | 解释  |
|  ----  | ----  | ----  | ----  |
| xasecure.audit.is.enabled | 否 | false | 是否开启审计功能，目前仅测试了log4j审计功能 |
| xasecure.audit.log4j.is.enabled | 否 | true | 开启log4j日志审计 |
| xasecure.audit.log4j.is.async  | 否 | true | 开启log4j的异步刷新机制 |
| xasecure.audit.log4j.async.max.queue.size  | 否 | 10240 |  |
| xasecure.audit.log4j.async.max.flush.interval.ms  | 否 | 30000 |  |

（5）修改ranger-obs-service-0.1.0/bin/start_server.sh脚本：
native_path=/home/modules/hadoop-3.1.1/lib/native/

3.启动nohup ranger-obs-service-0.1.0/bin/start_server.sh &

访问http://{ranger-obs-service}:26900/jmx:jvm和rpc接口监控信息

访问http://{ranger-obs-service}:26900/status：服务状态信息

#### 五.ranger-obs-client安装

1.将ranger-obs-client-0.1.0.jar安装在和hadoop-huaweicloud-3.1.1-hw-46.1.jar相同的位置

2.在hadoop的core-site.xml中进行配置

|  参数   | 是否必填  | 默认值  | 解释  |
|  ----  | ----  | ----  | ----  |
| fs.obs.authorize.provider  | 是 | 空 | hadoop-obs采用的鉴权类，配置为org.apache.hadoop.fs.obs.security.RangerAuthorizeProvider |
| ranger.obs.service.rpc.address  | 是 | 空 | ranger-obs-service服务地址，带端口号，当ranger.obs.client.impl为LoadBalanceRangerObsClientImpl时此配置项可以填写多个地址，并以分号分割 |
| fs.obs.authorize.fail.fallback  | 否 | false | 当鉴权失败时是否fallback |
| fs.obs.authorize.exception.fallback  | 否 | true | 当鉴权异常时是否fallback |
| ranger.obs.client.impl  | 否 | org.apache.ranger.obs.client.LoadBalanceRangerObsClientImpl | org.apache.ranger.obs.client.LoadBalanceRangerObsClientImpl带负载均衡能力的客户端和org.apache.ranger.obs.client.RangerObsClientImpl需要依赖外置负载均衡服务（例如nginx）的客户端 |
| ranger.obs.client.failover.max.retries  | 否 | 3 | 负载均衡客户端退避重试策略最大重试次数 |
| ranger.obs.client.failover.sleep.base.millis  | 否 | 100 | 负载均衡客户端退避重试策略间隔时间基数 |
| ranger.obs.client.failover.sleep.max.millis  | 否 | 2000 | 负载均衡客户端退避重试策略最大间隔时间 |
| ranger.obs.service.kerberos.principal  | 是（kerberos） | 无 | 安全集群配置项，ranger-obs-service服务的kerberos principal |
| ranger.obs.service.dt.service.name  | 是（kerberos）| 0.0.0.0:26901 | 安全集群配置项，请务必和ranger-obs-service中的ranger.obs.service.dt.service.name配置项保持一致 |


### 用户手册
1.ranger-admin配置

（1）进入obs service
![image](https://user-images.githubusercontent.com/10207782/175765246-5d5b8f0e-bee0-4795-9a0a-24304319be34.png)

（2）创建 policy

![image](https://user-images.githubusercontent.com/10207782/175765252-b1a46799-ef30-4ec3-9320-c8daad92d121.png)

相关参数含义如下：
- bucket：OBS桶名称
- path：对象路径，支持通配符，注意 对象路径不以/开始。
- include：表示设置的权限适用于 path 本身，还是除了 path 以外的其他路径。
- recursive：表示权限不仅适用于 path，还适用于 path 路径下的子成员（即递归子成员）。通常用于 path 设置为目录的情况。

注意：对于桶根目录，因为对应的对象路径为空字符串，目前仅能通过*通配符进行设置，建议仅为管理员设置根目录的权限

- user/group：用户名和用户组。这里是或的关系，即用户名或者用户组满足其中一个，即可拥有对应的操作权限。
- Permissions：
- Read：读操作。对应于对象存储里面的 GET、HEAD 类操作，包括下载对象、查询对象元数据等。
- Write：写操作。对应于对象存储里面的 PUT 类等修改操作，例如上传对象。

2.HDFS命令测试

执行各种hadoop fs命令验证是否符合预期，例如

hadoop fs -ls obs://obs-bucket/folder/


