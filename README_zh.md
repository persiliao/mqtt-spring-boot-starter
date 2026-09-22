# MQTT Spring Boot Starter（中文版）

[![](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![](https://img.shields.io/badge/Spring%20Boot-3.x-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![](https://img.shields.io/badge/Java-17+-orange.svg)](https://www.oracle.com/java)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.persiliao/mqtt-spring-boot-starter.svg)](https://search.maven.org/artifact/io.github.persiliao/mqtt-spring-boot-starter)

> English version: [README.md](README.md)

基于 [HiveMQ MQTT Client 5](https://github.com/hivemq/hivemq-mqtt-client) 的功能完备的 MQTT 客户端 Spring Boot Starter：自动配置、声明式消息处理、单/多服务器模式、TLS、断线自动重连与订阅恢复、异步与有序处理、按 Handler 维度的运行统计。

## 特性

- **开箱即用** — 通过 Spring Boot 自动配置零代码接入
- **单/多服务器** — `SINGLE` 与 `MULTI` 模式，支持每个服务器独立凭据
- **声明式 Handler** — Bean 上加 `@MqttMessageHandler` 注解即自动订阅、自动路由，无需手动接线
- **TLS** — `ssl://` 地址自动启用 TLS（默认信任库）
- **断线自动重连** — 客户端指数退避重连；每次（重）连接后自动（重新）建立订阅
- **异步处理** — Handler 运行在可配置的线程池上，不阻塞 MQTT 客户端线程
- **有序处理** — 按需启用按 topic 或按服务器连接的顺序保证
- **消息去重** — 可选基于内容指纹的去重，有界 LRU 窗口
- **JSON 反序列化** — `Map`/POJO 参数通过 Jackson 自动反序列化
- **运行统计** — 按 Handler 的成功/失败、耗时等计数器，可对外暴露监控
- **启动即校验** — 配置非法时应用上下文启动即失败，报错信息明确

## 环境要求

- Java 17+（编译为 Java 17 字节码，class file version 61）
- Spring Boot 3.x 及以上（版本由父 POM 继承，可通过 `${spring-boot.version}` 覆盖；构建强制拒绝 2.x）
- HiveMQ MQTT Client 1.3.x（传递依赖）

## 快速开始

### 1. 添加依赖

**Maven：**

```xml
<dependency>
    <groupId>io.github.persiliao</groupId>
    <artifactId>mqtt-spring-boot-starter</artifactId>
    <version>2026.1.1</version>
</dependency>
```

**Gradle：**

```groovy
implementation 'io.github.persiliao:mqtt-spring-boot-starter:2026.1.1'
```

### 2. 配置 Broker

```yaml
mqtt:
  enabled: true
  mode: SINGLE
  single-server:
    server-uri: tcp://localhost:1883
    client-id: my-app-${random.uuid}
    username: ${MQTT_USERNAME:}
    password: ${MQTT_PASSWORD:}
```

### 3. 声明 Handler

```java
@Component
@MqttMessageHandler(topics = "sensor/temperature")
public class TemperatureHandler {

    public void handleMessage(String topic, String payload) {
        System.out.println("收到 " + topic + " 的消息: " + payload);
    }
}
```

完成——Starter 会在连接建立后自动订阅 topic 并把消息路由到该方法。

## 配置项参考

前缀：`mqtt`

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `enabled` | `true` | 是否启用整个自动配置 |
| `mode` | `SINGLE` | `SINGLE` 或 `MULTI` |
| `async.core-pool-size` | `0` | 异步线程池核心线程数（`0` = CPU 核数） |
| `async.max-pool-size` | `0` | 异步线程池最大线程数（`0` = 2 × CPU 核数） |
| `async.queue-capacity` | `0` | 有界队列容量，满后触发背压（`0` = 1024） |
| `async.max-ordered-executors` | `64` | 有序执行器数量上限（见[顺序性与背压](#顺序性与背压)） |

### 服务器配置（`mqtt.single-server.*` / `mqtt.multi-server.servers[n].*`）

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `id` | `default` | 逻辑服务器 id（`MULTI` 模式必填；作为 serverId 传给 Handler） |
| `server-uri` | — | Broker 地址。支持 `tcp://` 与 `ssl://`（TLS）；省略协议默认 `tcp`；默认端口 1883 / 8883 |
| `client-id` | — | MQTT 客户端标识（必填，同一 Broker 下必须唯一） |
| `username` / `password` | — | MQTT 认证凭据 |
| `keep-alive` | `60` | 心跳间隔（秒，0–65535） |
| `session-expiry-interval` | `3600` | MQTT 5 会话过期时间（秒，0–4294967295） |
| `clean-start` | `false` | MQTT 5 干净启动标志 |
| `automatic-reconnect` | `true` | 客户端级自动重连（指数退避） |
| `initial-delay` | `1s` | 重连初始退避时间（支持 Spring Duration 格式，如 `500ms`、`2s`），不得超过 `max-delay` |
| `max-delay` | `30s` | 重连退避上限 |
| `receive-maximum` | `32` | MQTT 5 接收上限（1–65535） |
| `maximum-packet-size` | `8388608` | MQTT 5 最大报文（字节，必须为正数） |

所有取值在启动时即完成校验：越界或缺失会让上下文启动失败，并明确指出出错的配置项，而不是在运行期抛出来自 MQTT 协议栈的晦涩异常。

### 多服务器示例

```yaml
mqtt:
  mode: MULTI
  multi-server:
    fail-fast: false
    servers:
      - id: primary
        server-uri: tcp://primary.example.com:1883
        client-id: app-primary
        username: ${PRIMARY_MQTT_USER}
        password: ${PRIMARY_MQTT_PASS}
      - id: secondary
        server-uri: ssl://secondary.example.com:8883
        client-id: app-secondary
```

`fail-fast: true` 时任一服务器创建失败即启动失败；否则失败的服务器会被记录日志并跳过。

## 注解参考

`@MqttMessageHandler`（已元注解 `@Component`）：

| 属性 | 默认值 | 说明 |
|---|---|---|
| `topics` | — | 要订阅的 topic 过滤器（支持 `+`、`#` 通配符） |
| `qos` | `1` | 订阅 QoS（0、1、2） |
| `async` | `true` | 在异步线程池处理消息，而非 MQTT 客户端回调线程 |
| `serverId` | `""` | 只订阅指定服务器（`""`/`*` = 所有已配置服务器） |
| `serverIds` | `{}` | 订阅多个服务器（优先于 `serverId`） |
| `retainAsPublished` | `false` | 保留发布方的 retain 标志（MQTT 5） |
| `deduplicate` | `false` | 丢弃同一订阅上近期出现过的相同内容指纹的消息 |
| `ordering` | `NONE` | `NONE`、`PER_TOPIC` 或 `PER_CLIENT` 顺序保证 |
| `maxConcurrentMessages` | `0` | 有序执行器的积压队列容量（`0` = 内置默认值） |
| `autoDeserialize` | `true` | 将 JSON 负载反序列化到 `Map`/POJO 参数 |
| `maxPayloadSize` | `0` | 超过该大小的消息直接丢弃（`0` = 不限制） |
| `statistics` | `true` | 是否采集该 Handler 的处理统计 |
| `group` / `description` / `tags` | — | 元数据，用于文档与工具 |

### Handler 方法

Handler 方法必须是** public、非静态、实例方法，方法名以 `handle` 开头，且至少有一个可解析参数**。一个 Bean 声明多个 Handler 方法时按声明顺序依次尝试，第一个不抛异常的生效（可作为降级链使用）。

| 参数类型 | 解析结果 |
|---|---|
| `String` | topic（第一个参数）或 UTF-8 解码后的负载 |
| `byte[]` | 原始负载 |
| `Mqtt5Publish` | 完整的 MQTT 5 publish 消息 |
| `MqttMessageContext` | 含 `topic`、`payload`、`serverId` 与 `Mqtt5Publish` 的信封 |
| `Map` / POJO | JSON 负载反序列化结果（需要 `autoDeserialize = true` 且容器中有 `ObjectMapper`） |

```java
@Component
@MqttMessageHandler(topics = "device/+/status", qos = 2, serverIds = "primary")
public class DeviceStatusHandler {

    // 最简形式
    public void handleMessage(String topic, String payload) {
        // topic + 文本负载
    }

    // 带服务器上下文与完整消息
    public void handleDeviceStatus(MqttMessageContext context, Mqtt5Publish publish) {
        String fromServer = context.serverId();
        byte[] raw = context.payload();
    }
}
```

> **serverId 说明：** `SINGLE` 模式下为 `mqtt.single-server.id`（未配置时为 `default`）；`MULTI` 模式下为各服务器配置的 `id`。

## 消息处理流程

```
收到 publish 消息
  → 去重检查（开启 deduplicate 时）
  → 负载大小检查（maxPayloadSize > 0 时）
  → 分发：
      ordering = PER_TOPIC / PER_CLIENT → 每个 key 独立的单线程执行器
      async = true                      → 可配置的异步线程池
      async = false                     → MQTT 客户端回调线程（同步）
  → 调用 Handler 方法，第一个成功即止
  → 更新统计（开启 statistics 时）
```

- **异步线程池** — 守护线程、有界队列、caller-runs 背压（消息不会丢失）。
- **去重** — 按订阅维护有界 LRU（1000 条）内容指纹（topic + QoS + retain + 负载）。可缓解 QoS 1/2 重传，但**不能**替代业务幂等。

### 顺序性与背压

有序 Handler 会为**每个顺序 key 分配一个单线程执行器**；积压队列满时由 MQTT 客户端线程直接执行作为背压。

由于 key 取自**入站消息的实际 topic**，像 `sensor/#` 这样的通配符订阅会产生无界数量的 key —— 每个都占一个线程。为保证资源可控：

- 最多创建 `mqtt.async.max-ordered-executors` 个（默认 `64`）有序执行器；超出的 key 共享异步线程池，即这些 topic 不再保证顺序（仅告警一次）；
- 空闲执行器会释放线程，长期无消息的 topic 不再占用资源；
- 按订阅的去重窗口同样设上限（1024 个）。

若确实需要严格顺序，应使用更精确的 topic 过滤器，而不是一味调大上限。

## 发布消息

注入客户端 Bean（`singleMqttClient`，或 `multiMqttClients` Map）直接发布：

```java
@Component
@RequiredArgsConstructor
public class TemperaturePublisher {

    private final Mqtt5AsyncClient client;

    public void publish(String deviceId, double temperature) {
        client.publishWith()
                .topic("sensor/" + deviceId + "/temperature")
                .payload(String.valueOf(temperature).getBytes(StandardCharsets.UTF_8))
                .qos(MqttQos.AT_LEAST_ONCE)
                .send()
                .whenComplete((result, error) -> {
                    if (error != null) {
                        log.error("发布失败", error);
                    }
                });
    }
}
```

## 监控与统计

`MqttSubscriptionManager` 提供按 Handler 的统计（总数/成功/失败、成功率、平均/最大/最小耗时 ms、最近处理时间）：

```java
@RestController
@RequiredArgsConstructor
public class MqttStatsController {

    private final MqttSubscriptionManager subscriptionManager;

    @GetMapping("/mqtt/stats")
    public Map<String, Object> stats() {
        return subscriptionManager.getStatistics();
    }
}
```

`MqttClientRegistry` 提供连接状态：

```java
registry.connectionStatus();          // Map<serverId, connected>
registry.isConnected("primary");
```

## 停机行为

上下文关闭时，starter 会：

1. 依次关闭重试调度器、异步线程池与有序执行器（每个最多等待 5 秒）；
2. 为每个**处于连接状态**的客户端发送 `DISCONNECT` 报文，让 Broker 立即释放会话，而不必等到心跳超时或会话过期。失败仅按 debug 记录，不会导致关闭失败。

HiveMQ 客户端本身没有 `close()`（其线程为守护线程，随 JVM 退出），因此显式发送 `DISCONNECT` 才是滚动重启干净的关键。

## 架构

```
io.github.persiliao.mqtt
├── MqttMessageHandler              声明式注解（公共 API）
├── MqttMessageContext              Handler 参数信封
├── MqttMessageConversionException  负载转换失败异常
├── autoconfigure
│   ├── MqttAutoConfiguration       @AutoConfiguration，Bean 装配
│   └── properties.MqttProperties   @ConfigurationProperties("mqtt")
├── client
│   ├── MqttClientFactory           构建并连接客户端（TLS、退避、监听器）
│   └── MqttClientRegistry          客户端查找 + 连接状态 + 事件
└── handler
    ├── MqttMessageHandlerProcessor 上下文刷新时发现 @MqttMessageHandler Bean
    ├── MqttSubscriptionManager     订阅生命周期、重试、统计
    ├── MqttMessageDispatcher       去重 → 大小检查 → 分发 → 调用 → 统计
    ├── HandlerMethod / HandlerRegistration / HandlerStatistics / LruSet
```

**为什么 Handler 发现放在 `ContextRefreshedEvent`：** 此时所有单例（包括 MQTT 客户端 Bean）均已实例化、配置绑定完成，从根上消除了 Handler 与客户端之间的 Bean 创建顺序竞态。订阅在客户端 connected 事件时执行，因此无论首次异步连接还是之后的每次自动重连都能正确建立订阅。

## 常见问题

**1. 连接失败** — 检查 `server-uri`、`client-id` 唯一性与凭据：

```yaml
mqtt:
  single-server:
    server-uri: tcp://localhost:1883
    client-id: unique-client-id
    username: correct-username
    password: correct-password
```

**2. 消息未被处理**
- topic 过滤器必须与实际发布的 topic 匹配（通配符：`+` 一级，`#` 多级）
- 方法名必须以 `handle` 开头，且参数类型可解析（见上表）
- 打开调试日志：`logging.level.io.github.persiliao.mqtt: DEBUG`

**3. 性能调优**

```yaml
mqtt:
  async:
    core-pool-size: 4
    max-pool-size: 8
    queue-capacity: 2048
```

```java
// 同一 topic 串行处理，并限制积压
@MqttMessageHandler(topics = "orders/#", ordering = MqttMessageHandler.Ordering.PER_TOPIC, maxConcurrentMessages = 512)
```

**4. 长耗时 Handler** — 保持异步（`async = true`，默认值）；同步 Handler 会阻塞该连接的消息接收。

## 升级指南

### 从 2.x 升级到 3.0.0

3.0.0 为彻底重写，存在以下破坏性变更：

- **包结构迁移**
  - `io.github.persiliao.mqtt.hander.MqttMessageHandler` → `io.github.persiliao.mqtt.MqttMessageHandler`
  - Handler 机制 → `io.github.persiliao.mqtt.handler`
  - `MqttProperties` → `io.github.persiliao.mqtt.autoconfigure.properties`
- **删除从未生效的注解属性**：`timeout`、`errorHandling`、`priority`、`version`、`contentType`；`retain` 更名为 `retainAsPublished`
- **方法参数中的 `serverId` 不再支持** — 改用 `MqttMessageContext` 参数
- **`ssl://` 现在真正启用 TLS**（2.x 仅映射端口）
- **`statistics` 标志现在真实生效**（默认 `true`）
- **`PER_CLIENT` 排序现在按服务器连接分键**（2.x 实际按 topic 分键）
- Bean 名称保持不变：`singleMqttClient`、`multiMqttClients`

## 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 仓库
2. 创建特性分支（`git checkout -b feature/amazing-feature`）
3. 提交变更（`git commit -m 'Add amazing feature'`）
4. 推送分支（`git push origin feature/amazing-feature`）
5. 发起 Pull Request

## 许可证

本项目采用 MIT License — 详见 [LICENSE](LICENSE) 文件。

## 致谢

- [HiveMQ MQTT Client](https://github.com/hivemq/hivemq-mqtt-client) — 优秀的 MQTT 3/5 客户端库
- [Spring Boot](https://spring.io/projects/spring-boot)
- 所有贡献者与用户
