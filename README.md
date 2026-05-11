# MQTT Spring Boot Starter

[![](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![](https://img.shields.io/badge/Spring%20Boot-3.x-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![](https://img.shields.io/badge/Java-17+-orange.svg)](https://www.oracle.com/java)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.persiliao/mqtt-spring-boot-starter.svg)](https://search.maven.org/artifact/io.github.persiliao/mqtt-spring-boot-starter)

A feature-rich, enterprise-grade MQTT client Spring Boot Starter that supports automatic configuration, declarative message processing, and various enhanced features.

## ✨ Features

- 🚀 **Out-of-the-box** - Zero configuration setup with Spring Boot auto-configuration
- 🔧 **Multiple Modes** - Supports both single-server and multi-server modes
- 📡 **Declarative Programming** - Annotation-based message handlers, no manual subscription needed
- 🛡️ **High Reliability** - Built-in circuit breaker, message deduplication, and automatic reconnection
- 📊 **Monitoring & Statistics** - Comprehensive message processing statistics and performance monitoring
- 🔄 **Ordered Processing** - Supports ordered message processing by topic or client
- 🎯 **Smart Routing** - Supports wildcard topics and automatic parameter resolution
- ⚡ **High Performance** - Asynchronous processing, connection pooling, and method caching optimization
- 🔌 **Highly Extensible** - Supports custom validation, deserialization, and interceptors

## 📦 Requirements

- Java 17+
- Spring Boot 3.x
- Maven 3.6+ or Gradle 7.x
- HiveMQ MQTT Client 5.x

## 🚀 Quick Start

### 1. Add Dependency

**Maven:**
```xml
<dependency>
    <groupId>io.github.persiliao</groupId>
    <artifactId>mqtt-spring-boot-starter</artifactId>
    <version>3.0.0</version>
</dependency>
```

**Gradle:**
```groovy
implementation 'io.github.persiliao:mqtt-spring-boot-starter:3.0.0'
```

### 2. Basic Configuration

Add configuration to `application.yml`:

```yaml
mqtt:
  enabled: true
  mode: SINGLE
  single-server:
    server-uri: tcp://localhost:1883
    client-id: spring-app-${random.uuid}
    username: ${MQTT_USERNAME:}
    password: ${MQTT_PASSWORD:}
```

### 3. Create Message Handler

```java
@Slf4j
@Component
@MqttMessageHandler(topics = "sensor/temperature")
public class TemperatureHandler {
    
    public void handleMessage(String topic, String payload) {
        log.info("Received temperature data: topic={}, temperature={}°C", topic, payload);
    }
}
```

### 4. Run Application

Start the Spring Boot application, and the handler will automatically subscribe to the configured topics and process messages.

## ⚙️ Detailed Configuration

### Single Server Mode

```yaml
mqtt:
  enabled: true
  mode: SINGLE
  single-server:
    server-uri: tcp://mqtt.example.com:1883
    client-id: app-client-${spring.application.name}
    username: admin
    password: secret123
    keep-alive: 60
    session-expiry-interval: 86400
    clean-start: false
    automatic-reconnect: true
    initial-delay: 1
    max-delay: 30
```

### Multiple Servers Mode

```yaml
mqtt:
  enabled: true
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
        username: ${SECONDARY_MQTT_USER}
        password: ${SECONDARY_MQTT_PASS}
```

### Environment-Specific Configuration

```yaml
# application-dev.yml
mqtt:
  single-server:
    server-uri: tcp://localhost:1883
    client-id: dev-${random.uuid}
    clean-start: true
```

```yaml
# application-prod.yml
mqtt:
  mode: MULTI
  multi-server:
    servers:
      - id: prod-1
        server-uri: ${PROD_MQTT_URI_1}
        client-id: ${HOSTNAME}-prod-1
      - id: prod-2
        server-uri: ${PROD_MQTT_URI_2}
        client-id: ${HOSTNAME}-prod-2
```

## 📖 Annotation Usage

### Basic Usage

```java
@Slf4j
@Component
@MqttMessageHandler(topics = "home/living-room/temperature")
public class TemperatureHandler {
    
    public void handleMessage(String topic, String payload) {
        // Process temperature message
    }
}
```

### Support for Multiple Parameter Types

```java
@Component
@MqttMessageHandler(topics = "sensor/#")
public class SensorHandler {
    
    // Topic + String payload
    public void handleMessage(String topic, String payload) {
        // Process string message
    }
    
    // Raw byte data
    public void handleMessage(String topic, byte[] payload) {
        // Process binary message
    }
    
    // Complete MQTT message object
    public void handleMessage(Mqtt5Publish publish) {
        // Access all MQTT message properties
    }
    
    // Server identifier
    public void handleMessage(String topic, String payload, String serverId) {
        // Know which server the message came from
    }
}
```

### Advanced Feature Configuration

```java
@Component
@MqttMessageHandler(
    topics = {"device/+/status", "device/+/data"},
    qos = 2,
    serverIds = {"primary", "secondary"},
    async = true,
    retain = true,
    deduplicate = true,
    circuitBreaker = true,
    circuitBreakerThreshold = 30.0,
    ordering = MqttMessageHandler.Ordering.PER_TOPIC,
    validation = true,
    maxPayloadSize = 1024 * 1024,  // 1MB
    contentType = "application/json",
    autoDeserialize = true,
    statistics = true,
    description = "Device Status Monitoring Handler",
    tags = {"device", "monitoring"}
)
public class AdvancedDeviceHandler {
    
    public void handleMessage(String topic, Map<String, Object> payload, String serverId) {
        // Auto-deserialized JSON message
    }
}
```

## 🎯 Topic Pattern Support

Supports standard MQTT topic wildcards:

```java
// Single-level wildcard +
@MqttMessageHandler(topics = "home/+/temperature")
// Matches: home/living-room/temperature, home/bedroom/temperature
// Does NOT match: home/living-room/sensor/temperature

// Multi-level wildcard #
@MqttMessageHandler(topics = "home/#")
// Matches: home/living-room/temperature, home/kitchen/light/status
// Matches: home/floor1/room2/sensor/data
```

## 🔧 API Reference

### Publishing Messages

```java
@Slf4j
@Component
@RequiredArgsConstructor
public class MqttMessagePublisher {
    
    private final Mqtt5AsyncClient mqttClient;
    
    public void publishTemperature(String deviceId, double temperature) {
        String topic = "sensor/" + deviceId + "/temperature";
        String payload = String.valueOf(temperature);
        
        mqttClient.publishWith()
            .topic(topic)
            .payload(payload.getBytes())
            .qos(MqttQos.AT_LEAST_ONCE)
            .retain(false)
            .send()
            .whenComplete((result, throwable) -> {
                if (throwable != null) {
                    log.error("Failed to publish message: {}", throwable.getMessage(), throwable);
                } else {
                    log.debug("Message published successfully: {}", topic);
                }
            });
    }
}
```

### Getting Statistics

```java
@RestController
@RequiredArgsConstructor
public class MqttStatsController {
    
    private final MqttMessageHandlerProcessor processor;
    
    @GetMapping("/mqtt/stats")
    public Map<String, Object> getStats() {
        return processor.getSubscriptionStats();
    }
    
    @GetMapping("/mqtt/handlers/{handler}/stats")
    public Map<String, Object> getHandlerStats(@PathVariable String handler) {
        return processor.getHandlerStatistics(handler);
    }
}
```

## 🏗️ Architecture Design

### Core Components

```
┌─────────────────────────────────────────────────┐
│            MqttAutoConfiguration               │
│  ├─ singleMqttClient()                         │
│  ├─ multiMqttClients()                         │
│  └─ MqttMessageHandlerProcessor                │
└─────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────┐
│       MqttMessageHandlerProcessor              │
│  ├─ SubscriptionContext  (Subscription State)  │
│  ├─ EnhancedHandlerMethod (Enhanced Processing)│
│  ├─ CircuitBreaker       (Circuit Breaker)     │
│  ├─ HandlerStatistics    (Statistics)          │
│  └─ LimitedSizeSet       (Deduplication)       │
└─────────────────────────────────────────────────┘
```

### Message Processing Flow

```
1. MQTT message arrives
   ↓
2. Check for duplicate messages (if deduplication enabled)
   ↓
3. Check circuit breaker status (if circuit breaker enabled)
   ↓
4. Validate message (if validation enabled)
   ↓
5. Auto-deserialize (if enabled)
   ↓
6. Invoke handler method
   ↓
7. Update statistics
   ↓
8. Update circuit breaker status
```

## 🔍 Troubleshooting

### Common Issues

**1. Connection Failed**
```yaml
# Check configuration
mqtt:
  single-server:
    server-uri: tcp://localhost:1883  # Verify URI is correct
    client-id: unique-client-id       # Ensure unique
    username: correct-username        # If authentication required
    password: correct-password
```

**2. Messages Not Processed**
- Check if topic pattern matches correctly
- Verify handler method signature is correct
- Check if other handlers are intercepting messages

**3. Performance Issues**
```java
// Enable async processing
@MqttMessageHandler(async = true, maxConcurrentMessages = 10)

// Adjust ordering configuration
@MqttMessageHandler(ordering = MqttMessageHandler.Ordering.NONE)
```

### Debug Logging

```yaml
logging:
  level:
    io.github.persiliao.mqtt: DEBUG
    com.hivemq: INFO
```

## 📈 Monitoring Metrics

The starter provides the following monitoring metrics:

- Subscription status statistics
- Message processing success rate
- Average processing time
- Throughput (messages/second)
- Circuit breaker status
- Duplicate message count

## 🔄 Upgrade Guide

### From 1.x to 2.x

1. Update dependency version
2. Check configuration property changes
3. Verify annotation property compatibility
4. Test message processing logic

## 🤝 Contributing

Issues and Pull Requests are welcome!

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- https://github.com/hivemq/hivemq-mqtt-client - Excellent MQTT client library
- https://spring.io/projects/spring-boot - Excellent Java application framework
- All contributors and users

## 📞 Support

- Submit an https://github.com/persiliao/mqtt-spring-boot-starter/issues
- Check the https://github.com/persiliao/mqtt-spring-boot-starter/wiki documentation
- Join https://github.com/persiliao/mqtt-spring-boot-starter/discussions


