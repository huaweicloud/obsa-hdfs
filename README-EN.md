### Overview
![image](https://user-images.githubusercontent.com/10207782/175764329-da0d34e0-cd4d-4a66-ae9a-1ab3c8060c2f.png)

1.ranger-obs-plugin: This component provides HUAWEI CLOUD OBS plug-in for ranger admin and controls OBS service permissions. After the plug-in is deployed, users can fill in the corresponding permission policy in Ranger admin.

2.ranger-obs-service: This component provides the RPC service. It periodically obtains the permission policies related to OBS from ranger admin and verifies the permission locally after receiving the authentication request from hadoop-obs/ranger-obs-client . This service can be scaled out. That is, you can deploy multiple ranger-obs-service services for load balancing. The Nginx service can be deployed at the front end to provide a unified service entry.

3.ranger-obs-client: This component is usually integrated with the hadoop-obs component and forwards requests that require permission verification to ranger-obs-service.

### Source code compilation
1.Go to the ranger-obs directory.

2.mvn clean package -Dmaven.test.skip=true -Dhadoop.version=3.1.1 -Dranger.version=2.0.0 -Dhadoop.huaweicloud.version=3.1.1-hw-46.1

-Dhadoop.version: defines the dependent Hadoop version.

-Dranger.version: defines the dependent ranger version.

-Dhadoop.huaweicloud.version: defines the version of the hadoop-obs component.

Appendix: How Do I Obtain Hadoop-obs and Other Components from the Maven Repository?

Add the following Maven repository address to ranger-obs/pom.xml:
```
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
```
3.Generate the following components:

ranger-obs/ranger-obs-client/target/ranger-obs-client-0.1.0.jar

ranger-obs/ranger-obs-plugin/target/ranger-obs-plugin-0.1.0.tar.gz

ranger-obs/ranger-obs-service/target/ranger-obs-service-0.1.0.tar.gz


### Installation and Deployment Guide
- The non-security cluster is used as an example. It supports security clusters, and Kerberos-related configurations are required for security clusters.
- Hadoop-3.1.1, hadoop-huaweicloud-3.1.1-hw-46.1.jar, and ranger2.0.0 are used as examples. Other versions have not been tested. You need to perform compatibility tests by yourself.

#### I.Hadoop and hadoop-obs Installation and Configuration
1. Download and install Hadoop-3.1.1 to /home/modules/hadoop-3.1.1/

2. Download hadoop-obs
https://repo.huaweicloud.com/repository/maven/huaweicloudsdk/com/huaweicloud/obs/hadoop-huaweicloud/3.1.1-hw-46.1/

3. Install and configure hadoop-obs
https://support.huaweicloud.com/bestpractice-obs/obs_05_1507.html

#### II.ranger-admin and ranger-usersync installation

1. Download the ranger2.0.0 source code for compilation

(1) Download: http://archive.apache.org/dist/ranger/2.0.0/apache-ranger-2.0.0.tar.gz

(2) Compile: mvn -Pall -DskipTests clean compile package install

Obtain the related tar.gz file from the target directory after compilation.

2. Install ranger-admin

(1) Install MySQL and Python in advance

(2) Decompress ranger-2.0.0-admin.tar.gz to /home/modules/ranger/ranger-2.0.0-admin

(3) Configure the ranger-2.0.0-admin/install.properties file. The following settings are for reference only. You can adjust the configuration items as required

<details>
<summary>Expand View</summary>
<pre><code>
DB_FLAVOR=MYSQL
# Download mysql-connector-java-5.1.47.jar
SQL_CONNECTOR_JAR=/home/modules/ranger/ranger-2.0.0-admin/mysql-connector-java-5.1.47.jar
#User name, password, and address for connecting to the MySQL database
db_root_user=xxx
db_root_password=xxx
db_host=xxx

Name of the database to be created
db_name=xxx
db_user=xxx
db_password=xxx

#Password of the rangerAdmin component
rangerAdmin_password=xxx
#Password of the Tagsync component
rangerTagsync_password=xxx
#Password of the Usersync component
rangerUsersync_password=xxx
#Consistent with the preceding information
keyadmin_password=xxx

#Comment out the audit_store configuration item
#audit_store=solr

#HTTP address of the rangeradmin
policymgr_external_url=http://xxx:6080

#User and user group for starting the rangeradmin process
unix_user=rangeradmin
unix_user_pwd=rangeradmin
unix_group=ranger

#Kerberos authentication configuration of the rangeradmin component. The following configuration is only an example. Leave blank for a non-security cluster
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

(4) Install：ranger-2.0.0-admin/setup.sh

(5) Start：ranger-admin

Related logs are stored in ranger-2.0.0-admin/ews/logs

(6) Verification: Open a browser and open http://{rangeradmin}:6080/

Login information:

User name: admin

Password: rangerAdmin_password=xxx in the ranger-2.0.0-admin/install.properties configuration file



3. (Optional) Install ranger-usersync

(1) Decompress ranger-2.0.0-usersync.tar.gz to /home/modules/ranger/ranger-2.0.0-usersync

(2) Configure the ranger-2.0.0-usersync/install.properties file. The following configuration items are for reference only. You can adjust the configuration items as required.

<details>
<summary>Expand View</summary>
<pre><code>
#URL of the rangeradmin
POLICY_MGR_URL =http://{rangeradmin}:6080

#Synchronize user information from the UNIX operating system.
SYNC_SOURCE = unix
#Synchronization interval, in minutes.
SYNC_INTERVAL = 1

#Linux user that runs the process.
unix_user=rangerusersync
unix_group=ranger

#Password of the rangerUserSync user. For details, see the configuration in the install.properties file in ranger-admin.
rangerUsersync_password=xxx

#Kerberos-related configuration. The following configuration is only an example. For non-security clusters, leave this parameter blank.
usersync_principal=rangerusersync/ecs-bigdata-obs.novalocal@NOVALOCAL
usersync_keytab=/etc/security/keytabs/rangerusersync.keytab
hadoop_conf=/home/modules/hadoop-3.1.1/etc/hadoop
</code></pre>
</details>

(3) Install：ranger-2.0.0-usersync/setup.sh

(4) Configuring the /etc/ranger/usersync/conf/ranger-ugsync-site.xml
```
<property>
<name>ranger.usersync.enabled</name>
<value>true</value>
</property>
```

(5) Start：ranger-usersync

Log files are stored in ranger-2.0.0-usersync/logs


#### III.ranger-obs-plugin installation
1. Decompress ranger-obs-plugin-0.1.0.tar.gz to /home/modules/ranger/ranger-obs-plugin-0.1.0

2. Place the JAR package in the ranger-2.0.0-admin installation directory

Create the obs directory in the ranger-2.0.0-admin/ews/webapp/WEB-INF/classes/ranger-plugins/ directory

Copy ranger-obs-plugin-0.1.0.jar to the ranger-2.0.0-admin/ews/webapp/WEB-INF/classes/ranger-plugins/obs directory

Note: Ensure that the owner and permission of the obs directory are the same as those of other plug-in directories

3. Restart the ranger-admin service.

ranger-admin restart

4. Register the OBS service on the ranger-admin

cd ranger-obs-plugin-0.1.0

curl -v -k -u{user}:{password} -X POST -H "Accept:application/json" -H "Content-Type:application/json" -d @./ranger-obs.json http://{rangeradmin}:6080/service/plugins/definitions

The values of user and password are the user and password for logging in to the rangeradmin page.

5. Create an OBS service on the following page:

![image](https://user-images.githubusercontent.com/10207782/175764704-449cc7ce-6f32-4a1f-906c-0777a24444e0.png)

![image](https://user-images.githubusercontent.com/10207782/175764578-0457eb75-701b-4cf6-b429-38d5197c8da6.png)

#### IV.Ranger-obs-service Installation
1. Decompress ranger-obs-service-0.1.0.tar.gz to /home/modules/ranger/ranger-obs-service-0.1.0

(1) bin: script directory

star_rpc_server.sh: This script is used for integration with the HUAWEI CLOUD MRS service. The script contains information specific to the MRS service.

start_server.sh: used by open-source big data clusters

(2) conf: configuration file directory

core-site.xml and hdfs-site.xml: configuration files required for accessing the HDFS service (this service depends on the HDFS service).

ranger-obs-security.xml and ranger-obs-audit.xml: configuration files for accessing the rangerAdmin service

ranger-obs.xml: main configuration file of the ranger-obs-service service

log4j.properties: log configuration file

(3) lib: directory of the dependent JAR package

2. Configuration

(1) Synchronize core-site.xml and hdfs-site.xml in hadoop-3.1.1 to ranger-obs-service-0.1.0/conf.

（2）ranger-obs.xml
| Parameter | Mandatory | Default value | Description |
|  ----  | ----  | ----  | ----  |
| ranger.obs.service.rpc.address | No | 0.0.0.0:26901 | ranger-obs-service service RPC listening address |
| ranger.obs.service.status.port | No | 26900 | HTTP listening port of the ranger-obs-service service |
| ranger.obs.service.authorize.enable | No | True | Indicates whether to enable permission interception. If this parameter is set to FALSE, all permission check is bypassed. |
| ranger.obs.service.sts.enable | No | false | Indicates whether to enable the STS service. This function is an experimental function. |
| ranger.obs.service.sts.provider | No | Empty | AK/SK or temporary AK/SK provider. The provider needs to implement the org.apache.ranger.obs.security.sts.STSProvider interface. |
| ranger.obs.service.kerberos.principal | Yes (kerberos) | None | Security cluster configuration item, running user of the ranger-obs-service service |
| ranger.obs.service.kerberos.keytab | Yes (kerberos) | None | Security cluster configuration item, keytab corresponding to the running user of the ranger-obs-service service |
| ranger.obs.service.dt.service.name | No | 0.0. 0.0:26901 | Security cluster configuration item, service name in DelegationToken. The ranger-obs-service service can be scaled out. Therefore, this configuration item must be the same for all ranger-obs-services. |
| ranger.obs.service.dt.manager | No | org.apache.ranger.obs.security.token.SimpleSecretManager | security cluster configuration item. SimpleSecretManager manages delegation tokens without storing tokens.|
| ranger.obs.service.dt.secret.provider | No | org.apache.ranger.obs.security.token.ShareFileSecretProvider | security cluster configuration item, which is the key provider of SimpleSecretManager |
| ranger.obs.service.dt.secret.file | No | Empty | Security cluster configuration item, which specifies the storage path of the key file loaded by the ShareFileSecretProvider key provider in HDFS. If the key file location is not configured, the key file location is automatically generated and stored in hdfs://xxx/${user}/secret.jceks by default. |
| ranger.obs.service.dt.renew-interval | No | 86400000 | Security cluster configuration item, which indicates the delegation token renewal interval. The unit is ms. The default value is 86400000 (one day). |
| ranger.obs.service.dt.max-lifetime | No | 604800000 | Security cluster configuration item, which indicates the delegation token renewal interval. The unit is ms. The default value is 604800000 (seven days). |
| ranger.plugin.obs.service.name | No | obs | The value must correspond to the service name of the OBS plug-in in rangerAdmin. |
| ranger.obs.service.rpc.handler.count | No | 10 | The number of RPC server handler threads |

（3）ranger-obs-security.xml
| Parameter | Mandatory | Default value | Description |
|  ----  | ----  | ----  | ----  |
| ranger.plugin.obs.policy.rest.url | Yes | Empty | URL of rangerAdmin |
| ranger.plugin.obs.policy.cache.dir | Yes | Empty | | The policy obtained from the rangerAdmin service is stored in the local cache directory. A cache directory is automatically created. |
| ranger.plugin.obs.policy.source.impl | No | org.apache.ranger.admin.client.RangerAdminRESTClient | |
| ranger.plugin.obs.policy.pollIntervalMs | No | Policy Pull Interval | 30000 |Policy pull interval |
| ranger.plugin.obs.policy.rest.client.connection.timeoutMs | No | 120000 | Access to rangerAdmin connection timed out |
| ranger.plugin.obs.policy.rest.client.read.timeoutMs | No | 30000 | Access to rangerAdmin read timed out |
| ranger.plugin.obs.service.name | No | obs | This parameter must correspond to the service name of the OBS plug-in in rangerAdmin |


（4）ranger-obs-audit.xml
| Parameter | Mandatory | Default value | Description |
|  ----  | ----  | ----  | ----  |
| xasecure.audit.is.enabled | No | false | Whether to enable the audit function. Currently, only the log4j audit function is tested |
| xasecure.audit.log4j.is.enabled | No | True | Log4j log audit is enabled |
| xasecure.audit.log4j.is.async | No | True | Enable the asynchronous update mechanism of log4j |
| xasecure.audit.log4j.async.max.queue.size | No | 10240 | |
| xasecure.audit.log4j.async.max.flush.interval.ms | No | 30000 | |

(5) Modify the ranger-obs-service-0.1.0/bin/start_server.sh

native_path=/home/modules/hadoop-3.1.1/lib/native/

3. Start: nohup ranger-obs-service-0.1.0/bin/start_server.sh &

Access http://{ranger-obs-service}:26900/jmx:jvm and RPC Interface Monitoring Information

Access http://{ranger-obs-service}:26900/status: service status information

#### V.ranger-obs-client Installation

1. Install ranger-obs-client-0.1.0.jar in the same location as hadoop-huaweicloud-3.1.1-hw-46.1.jar.

2. Configure the core-site.xml file of the Hadoop

| Parameter | Mandatory | Default value | Description |
|  ----  | ----  | ----  | ----  |
| fs.obs.authorize.provider | Yes | Empty | Authentication class used by hadoop-obs. **Set this parameter to org.apache.hadoop.fs.obs.security.RangerAuthorizeProvider** |
| ranger.obs.service.rpc.address | Yes | null | ranger-obs-service service address with a port number. When ranger.obs.client.impl is set to LoadBalanceRangerObsClientImpl, you can enter multiple addresses and separate them with semicolons (;). |
| fs.obs.authorize.fail.fallback | No | false | Indicates whether to fallback when authentication fails. |
| fs.obs.authorize.exception.fallback | No | true | Indicates whether to fallback when an authentication exception occurs. |
| ranger.obs.client.impl | No | org.apache.ranger.obs.client.LoadBalanceRangerObsClientImpl | org.apache.ranger.obs.client.LoadBalanceRangerObsClientImpl Clients with load balancing capabilities and org.apache.ranger.obs.client.RangerObsClientImpl need to depend on clients of external load balancing services (such as Nginx). |
| ranger.obs.client.failover.max.retries | No | 3 | Maximum number of retry times for load balancing client backoff retry policy |
| ranger.obs.client.failover.sleep.base.millis | No | 100 | Load Balancing Client Backoff Retry Interval Base |
| ranger.obs.client.failover.sleep.max.millis | No | 2000 | Maximum interval for backoff retry of the load balancing client |
| ranger.obs.service.kerberos.principal | Yes (kerberos) | None | Security cluster configuration item, Kerberos principal of the ranger-obs-service service |
| ranger.obs.service.dt.service.name | Yes (kerberos) | 0.0.0.0:26901 | Security cluster configuration item. The value must be the same as the value of ranger.obs.service.dt.service.name in ranger-obs-service. |

###User Manual
1. Configure the ranger-admin

(1) Go to the OBS service page
![image](https://user-images.githubusercontent.com/10207782/175765246-5d5b8f0e-bee0-4795-9a0a-24304319be34.png)

(2) Create a policy

![image](https://user-images.githubusercontent.com/10207782/175765252-b1a46799-ef30-4ec3-9320-c8daad92d121.png)

The parameters are described as follows:
- bucket: specifies the name of an OBS bucket
- path: indicates the object path. Wildcard characters are supported. Note that the object path does not start with a slash (/)
- include: indicates that the permission applies to the path itself or other paths
- recursive: indicates that the permission applies not only to path but also to sub-members (recursive sub-members) in the path. This parameter is usually used when path is set to a directory.

Note: For the root directory of a bucket, the corresponding object path is an empty string. Currently, only the wildcard * can be used to set the root directory. You are advised to set the root directory permission only for the administrator.

- user/group: user name and user group. The relationship between the user name and user group is OR. That is, if the user name or user group meets either of them, the user has the corresponding operation rights.
- Permissions:
- Read: read operation. It corresponds to GET and HEAD operations in OBS, including downloading objects and querying object metadata.
- Write: write operation. It corresponds to PUT operations in OBS, for example, uploading an object.

2. HDFS command test

Run various Hadoop fs commands to check whether the command meets the expectation. For example:

hadoop fs -ls obs://obs-bucket/folder/


