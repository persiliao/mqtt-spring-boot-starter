# MQTT Spring Boot Starter

[![](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![](https://img.shields.io/badge/Spring%20Boot-3.x-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![](https://img.shields.io/badge/Java-17+-orange.svg)](https://www.oracle.com/java)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.persiliao/mqtt-spring-boot-starter.svg)](https://search.maven.org/artifact/io.github.persiliao/mqtt-spring-boot-starter)

> 中文版: [README_zh.md](README_zh.md)

A feature-rich MQTT client Spring Boot Starter built on the [HiveMQ MQTT Client 5](https://github.com/hivemq/hivemq-mqtt-client): automatic configuration, declarative message handling, single/multi-server modes, TLS, automatic reconnection with subscription recovery, async and ordered processing, and per-handler statistics.

## Features

- **Out-of-the-box** — zero-configuration setup via Spring Boot auto-configuration
- **Single & multi-server** — `SINGLE` and `MULTI` modes with per-server credentials
- **Declarative handlers** — annotate a bean with `@MqttMessageHandler`; topics are subscribed automatically, no manual wiring
- **TLS** — `ssl://` server URIs negotiate TLS with the default trust store
- **Automatic reconnection** — client-level exponential backoff; subscriptions are (re)established on every (re)connection
- **Async processing** — handlers run on a tunable thread pool so the MQTT client thread is never blocked
- **Ordered processing** — per-topic or per-client ordering guarantees on demand
- **Duplicate suppression** — optional content-based deduplication with a bounded LRU window
- **JSON deserialization** — `Map`/POJO handler parameters deserialized via Jackson
- **Statistics** — per-handler counters (success/failure, latency, throughput inputs) exposed for monitoring
- **Fail-fast validation** — invalid configuration fails the application context at startup with a clear message

## Requirements

- Java 17+ (compiled to Java 17 bytecode, class file version 61)
- Spring Boot 3.x or higher (the version is inherited from the parent and overridable via `${spring-boot.version}`; the build rejects any 2.x release)
- HiveMQ MQTT Client 1.3.x (pulled in transitively)

## Quick Start

### 1. Add the dependency

**Maven:**

```xml
<dependency>
    <groupId>io.github.persiliao</groupId>
    <artifactId>mqtt-spring-boot-starter</artifactId>
    <version>2026.1.1</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'io.github.persiliao:mqtt-spring-boot-starter:2026.1.1'
```

### 2. Configure the broker

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

### 3. Declare a handler

```java
@Component
@MqttMessageHandler(topics = "sensor/temperature")
public class TemperatureHandler {

    public void handleMessage(String topic, String payload) {
        System.out.println("Received on " + topic + ": " + payload);
    }
}
```

That is all — the starter subscribes the topic on connection and routes incoming messages to the method.

## Configuration Reference

Prefix: `mqtt`

| Property | Default | Description |
|---|---|---|
| `enabled` | `true` | Enables the whole auto-configuration |
| `mode` | `SINGLE` | `SINGLE` or `MULTI` |
| `async.core-pool-size` | `0` | Core pool size of the async executor (`0` = available processors) |
| `async.max-pool-size` | `0` | Max pool size (`0` = 2x available processors) |
| `async.queue-capacity` | `0` | Bounded queue capacity before backpressure (`0` = 1024) |

### Server configuration (`mqtt.single-server.*` / `mqtt.multi-server.servers[n].*`)

| Property | Default | Description |
|---|---|---|
| `id` | `default` | Logical server id (required in `MULTI` mode; passed to handlers as server id) |
| `server-uri` | — | Broker address. Schemes: `tcp://` and `ssl://` (TLS). Missing scheme = `tcp`. Default ports: 1883 / 8883 |
| `client-id` | — | MQTT client identifier (required, must be unique per broker) |
| `username` / `password` | — | MQTT authentication credentials |
| `keep-alive` | `60` | Keep-alive interval in seconds |
| `session-expiry-interval` | `3600` | MQTT 5 session expiry in seconds |
| `clean-start` | `false` | MQTT 5 clean start flag |
| `automatic-reconnect` | `true` | Client-level automatic reconnection with exponential backoff |
| `initial-delay` | `1s` | Initial reconnection backoff delay (any Spring `Duration` format, e.g. `500ms`, `2s`) |
| `max-delay` | `30s` | Upper bound of the reconnection backoff |
| `receive-maximum` | `32` | MQTT 5 receive maximum |
| `maximum-packet-size` | `8388608` | MQTT 5 maximum packet size in bytes |

### Multi-server example

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

With `fail-fast: true` the context fails to start if any server cannot be created; otherwise failing servers are logged and skipped.

## Annotation Reference

`@MqttMessageHandler` (meta-annotated with `@Component`):

| Attribute | Default | Description |
|---|---|---|
| `topics` | — | Topic filters to subscribe to (supports `+` and `#` wildcards) |
| `qos` | `1` | QoS of the subscriptions (0, 1, 2) |
| `async` | `true` | Process messages on the async pool instead of the MQTT client thread |
| `serverId` | `""` | Subscribe to a single server (`""`/`*` = all configured servers) |
| `serverIds` | `{}` | Subscribe to multiple servers (takes precedence over `serverId`) |
| `retainAsPublished` | `false` | Preserve the published retain flag (MQTT 5) |
| `deduplicate` | `false` | Drop messages whose content fingerprint was seen recently on the same subscription |
| `ordering` | `NONE` | `NONE`, `PER_TOPIC`, or `PER_CLIENT` ordering guarantee |
| `maxConcurrentMessages` | `0` | Backlog queue capacity of the ordered executor (`0` = built-in default) |
| `autoDeserialize` | `true` | Deserialize JSON payloads into `Map`/POJO parameters |
| `contentType` | `application/json` | Declared payload content type |
| `maxPayloadSize` | `0` | Drop messages larger than this (`0` = no limit) |
| `statistics` | `true` | Collect per-handler processing statistics |
| `group` / `description` / `tags` | — | Metadata for documentation and tooling |

### Handler methods

Handler methods are **public, non-static instance methods whose name starts with `handle`** and which declare at least one resolvable parameter. When a bean declares several, they are tried in declaration order and the first one that completes without throwing wins (a convenient fallback chain).

| Parameter type | Resolved value |
|---|---|
| `String` | the topic (first parameter) or the payload decoded as UTF-8 |
| `byte[]` | the raw payload |
| `Mqtt5Publish` | the full MQTT 5 publish message |
| `MqttMessageContext` | envelope with `topic`, `payload`, `serverId` and the `Mqtt5Publish` |
| `Map` / POJO | the payload deserialized from JSON (requires `autoDeserialize = true` and an `ObjectMapper` bean) |

```java
@Component
@MqttMessageHandler(topics = "device/+/status", qos = 2, serverIds = "primary")
public class DeviceStatusHandler {

    // Simplest form
    public void handleMessage(String topic, String payload) {
        // topic + text payload
    }

    // With server context and the full message
    public void handleDeviceStatus(MqttMessageContext context, Mqtt5Publish publish) {
        String fromServer = context.serverId();
        byte[] raw = context.payload();
    }
}
```

> **Note on server ids:** in `SINGLE` mode the server id is `mqtt.single-server.id` (or `default`); in `MULTI` mode it is the configured `id` of each server.

## Message Processing Flow

```
Incoming publish
  → duplicate check (if deduplicate)
  → payload size check (if maxPayloadSize > 0)
  → dispatch:
      ordering = PER_TOPIC / PER_CLIENT → dedicated single-thread executor per key
      async = true                       → configurable async pool
      async = false                      → MQTT client callback thread (synchronous)
  → invoke handler methods, first success wins
  → update statistics (if statistics)
```

- **Async pool** — daemon threads, bounded queue, caller-runs backpressure (messages are never dropped).
- **Ordered executors** — one single-thread executor per (topic, handler) or (server, handler) key; a full backlog queue makes the MQTT client thread run the handler itself as backpressure.
- **Deduplication** — per subscription, a bounded LRU (1000 entries) of content fingerprints (topic + QoS + retain + payload). It mitigates QoS 1/2 re-delivery; it does **not** make handlers idempotent.

## Publishing Messages

Inject the client bean (`singleMqttClient`, or the `multiMqttClients` map) and publish directly:

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
                        log.error("Publish failed", error);
                    }
                });
    }
}
```

## Monitoring & Statistics

`MqttSubscriptionManager` exposes per-handler statistics (total/success/failure, success rate, average/max/min latency in ms, last processed timestamp):

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

`MqttClientRegistry` exposes connection state:

```java
registry.connectionStatus();          // Map<serverId, connected>
registry.isConnected("primary");
```

## Architecture

```
io.github.persiliao.mqtt
├── MqttMessageHandler              declarative annotation (public API)
├── MqttMessageContext              handler parameter envelope
├── MqttMessageConversionException  payload conversion failure
├── autoconfigure
│   ├── MqttAutoConfiguration       @AutoConfiguration, bean wiring
│   └── properties.MqttProperties   @ConfigurationProperties("mqtt")
├── client
│   ├── MqttClientFactory           builds & connects clients (TLS, backoff, listeners)
│   └── MqttClientRegistry          client lookup + connection state + events
└── handler
    ├── MqttMessageHandlerProcessor discovers @MqttMessageHandler beans on context refresh
    ├── MqttSubscriptionManager     subscription lifecycle, retry, statistics
    ├── MqttMessageDispatcher       dedup → size check → dispatch → invoke → stats
    ├── HandlerMethod / HandlerRegistration / HandlerStatistics / LruSet
```

**Why the handler discovery happens on `ContextRefreshedEvent`:** at that point every singleton — including the MQTT client beans — is already instantiated and configuration is bound, which removes the bean-creation ordering race between handlers and clients. Subscriptions are then performed on each client's *connected* event, so they survive the initial (asynchronous) connection and every automatic reconnect.

## Troubleshooting

**1. Connection fails** — check `server-uri`, `client-id` uniqueness and credentials:

```yaml
mqtt:
  single-server:
    server-uri: tcp://localhost:1883
    client-id: unique-client-id
    username: correct-username
    password: correct-password
```

**2. Messages are not processed**
- The topic filter must match the actual published topic (wildcards: `+` one level, `#` multi level)
- The method name must start with `handle` and its parameters must be resolvable (see the table above)
- Enable debug logging: `logging.level.io.github.persiliao.mqtt: DEBUG`

**3. Performance tuning**

```yaml
mqtt:
  async:
    core-pool-size: 4
    max-pool-size: 8
    queue-capacity: 2048
```

```java
// Serialize messages of the same topic; bound the backlog
@MqttMessageHandler(topics = "orders/#", ordering = MqttMessageHandler.Ordering.PER_TOPIC, maxConcurrentMessages = 512)
```

**4. Long-running handlers** — keep them async (`async = true`, the default); a synchronous handler blocks message ingestion for that connection.

## Upgrade Guide

### From 2.x to 3.0.0

3.0.0 is a ground-up rewrite. Breaking changes:

- **Packages moved**
  - `io.github.persiliao.mqtt.hander.MqttMessageHandler` → `io.github.persiliao.mqtt.MqttMessageHandler`
  - handler machinery → `io.github.persiliao.mqtt.handler`
  - `MqttProperties` → `io.github.persiliao.mqtt.autoconfigure.properties`
- **Annotation attributes removed** (never functional): `timeout`, `errorHandling`, `priority`, `version`; `retain` renamed to `retainAsPublished`
- **`serverId` method parameter no longer exists** — use the `MqttMessageContext` parameter instead
- **`ssl://` now performs real TLS** (2.x only mapped the port)
- **`statistics` flag is now honored** (default `true`)
- **`PER_CLIENT` ordering now orders per server connection** (2.x keyed it per topic)
- Bean names are unchanged: `singleMqttClient`, `multiMqttClients`

## Contributing

Issues and Pull Requests are welcome!

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file.

## Acknowledgments

- [HiveMQ MQTT Client](https://github.com/hivemq/hivemq-mqtt-client) — excellent MQTT 3/5 client library
- [Spring Boot](https://spring.io/projects/spring-boot)
- All contributors and users
