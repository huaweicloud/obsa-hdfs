### 概述
### 概述
 ![image](https://user-images.githubusercontent.com/10207782/175764329-da0d34e0-cd4d-4a66-ae9a-1ab3c8060c2f.png)

1.ranger-obs-plugin：提供 Ranger 服务端的服务定义插件。它提供了Ranger侧的OBS服务权限控制；部署了该插件后，用户即可在 Ranger 的控制页面上，填写相应的权限策略。


2.ranger-obs-service：该服务提供RPC接口，其在收到hadoop-obs/ranger-obs-client的鉴权请求后在本地进行权限校验;其会周期性从 Ranger 服务端同步权限策略


3.ranger-obs-client：hadoop-obs集成此插件，把需要权限校验的请求转发给ranger-obs-service。

### 源码编译 
1.进行ranger-obs目录


2.mvn clean package -Dmaven.test.skip=true


3.生成如下组件


|  参数   | 是否必填  | 默认值  | 解释  |
|  ----  | ----  | ----  | ----  |
| fs.obs.authorize.provider  | 是 | 空 | hadoop-obs采用的鉴权类 |
| fs.obs.authorize.provider  | 是 | 空 | ranger-obs-service服务地址，当ranger.obs.client.impl为LoadBalanceRangerObsClientImpl时此配置项可以填写多个地址，并以分号分割 |
| fs.obs.authorize.fail.fallback  | 否 | false | 当鉴权失败时是否fallback |
| fs.obs.authorize.exception.fallback  | 否 | true | 当鉴权异常时是否fallback |
| ranger.obs.client.impl  | 否 | org.apache.ranger.obs.client.LoadBalanceRangerObsClientImpl | org.apache.ranger.obs.client.LoadBalanceRangerObsClientImpl：带负载均衡能力的客户端
和org.apache.ranger.obs.client.RangerObsClientImpl：需要依赖外置负载均衡服务的客户端 |
| ranger.obs.client.failover.max.retries  | 否 | 3 | 负载均衡客户端退避重试策略最大重试次数 |
| ranger.obs.client.failover.sleep.base.millis  | 否 | 100 | 负载均衡客户端退避重试策略间隔时间基数 |
| ranger.obs.client.failover.sleep.max.millis  | 否 | 2000 | 负载均衡客户端退避重试策略最大间隔时间 |
| ranger.obs.service.kerberos.principal  | 是（kerberos） | 无 | 安全集群配置项，ranger-obs-service服务的kerberos principal |
| ranger.obs.service.dt.service.name  | 是（kerberos）| 0.0.0.0:26901 | 安全集群配置项，请务必和ranger-obs-service中的ranger.obs.service.dt.service.name配置项保持一致 |


