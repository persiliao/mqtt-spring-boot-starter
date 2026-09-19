package io.github.persiliao.mqtt;

import org.springframework.stereotype.Component;
import org.springframework.core.annotation.AliasFor;

import java.lang.annotation.*;

/**
 * MQTT Message Handler Annotation
 * <p>
 * This annotation marks a Spring component as an MQTT message handler.
 * It enables automatic subscription to specified MQTT topics and message processing.
 * <p>
 * The annotated class will be automatically discovered and registered by
 * {@link MqttMessageHandlerProcessor}.
 * <p>
 * Usage examples:
 * 1. Simple handler:
 * {@code
 * @MqttMessageHandler(topics = "home/temperature")
 * public class TemperatureHandler {
 *     public void handleMessage(String topic, String payload) {
 *         // handle message
 *     }
 * }
 * }
 *
 * 2. Multiple topics with QoS 2:
 * {@code
 * @MqttMessageHandler(topics = {"sensor/#", "device/+/status"}, qos = 2)
 * public class SensorHandler {
 *     public void handleMessage(String topic, byte[] payload, Mqtt5Publish publish) {
 *         // handle message
 *     }
 * }
 * }
 *
 * 3. Specific server with async disabled:
 * {@code
 * @MqttMessageHandler(
 *     topics = "alerts/#",
 *     serverId = "primary-broker",
 *     async = false,
 *     retain = true
 * )
 * public class AlertHandler {
 *     public void handleMessage(String topic, String payload, String serverId) {
 *         // handle message synchronously
 *     }
 * }
 * }
 *
 * @see MqttMessageHandlerProcessor
 * @since 1.0.0
 * @author YourName
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface MqttMessageHandler {

    /**
     * MQTT topics to subscribe to
     * <p>
     * Supports MQTT topic wildcards:
     * - '+' (single-level wildcard): matches one topic level
     *   Example: "home/+/temperature" matches "home/living-room/temperature"
     * - '#' (multi-level wildcard): matches multiple topic levels
     *   Example: "home/#" matches "home/living-room/temperature" and "home/kitchen/light/status"
     * <p>
     * For multiple topics, use an array: {"topic1", "topic2"}
     *
     * @return array of topic patterns to subscribe
     */
    String[] topics() default {};

    /**
     * Quality of Service (QoS) level for subscriptions
     * <p>
     * Valid values: 0, 1, 2
     * - 0: At most once delivery (fire and forget)
     * - 1: At least once delivery (guaranteed delivery, may duplicate)
     * - 2: Exactly once delivery (guaranteed, no duplicates)
     * <p>
     * Default: 1 (at least once)
     *
     * @return QoS level
     */
    int qos() default 1;

    /**
     * Enable asynchronous message processing
     * <p>
     * If true, messages are dispatched to a dedicated internal thread pool
     * (see mqtt.async.*) so the MQTT client's network thread is never blocked
     * by handler logic. The handler method should be thread-safe when async is enabled.
     * <p>
     * If false, messages are processed synchronously on the MQTT client's callback
     * thread. Long-running handlers may then stall the connection and cause backpressure.
     * <p>
     * Default: true
     *
     * @return true for asynchronous processing, false for synchronous
     */
    boolean async() default true;

    /**
     * Server identifier for single server mode
     * <p>
     * Specifies which MQTT server to connect to. This is used when:
     * 1. mode = SINGLE: can be empty or any value (uses the single server)
     * 2. mode = MULTI: specifies which server in the multi-server configuration
     * <p>
     * For multiple servers, use {@link #serverIds()} instead.
     * If both serverId and serverIds are specified, serverIds takes precedence.
     * <p>
     * Special values:
     * - "" (empty string): uses default server
     * - "*": subscribes to all servers (in MULTI mode)
     *
     * @return server identifier
     */
    String serverId() default "";

    /**
     * Server identifiers for multiple servers
     * <p>
     * Specifies multiple MQTT servers to subscribe to. This is useful when:
     * 1. You want to subscribe to the same topics on multiple servers
     * 2. You have a failover or load balancing setup
     * <p>
     * Format: comma-separated server IDs or array
     * Example: {"server1", "server2"} or "server1,server2"
     * <p>
     * If this is specified, {@link #serverId()} is ignored.
     * <p>
     * Special values:
     * - "*": subscribes to all servers
     * - "": uses default server
     *
     * @return array of server identifiers
     */
    String[] serverIds() default {};

    /**
     * Enable retain-as-published flag for subscriptions
     * <p>
     * If true, the retain flag on received messages is set as it was published.
     * This is useful when you need to distinguish between real-time and retained messages.
     * <p>
     * MQTT 5.0 feature: Controls whether the server forwards the retain flag as published.
     * <p>
     * Default: false
     *
     * @return true to preserve retain flag, false to clear it
     */
    boolean retain() default false;

    /**
     * Alias for Spring's @Component value attribute
     * <p>
     * Allows specifying the bean name for the annotated component.
     * If not specified, the bean name will be generated automatically.
     * <p>
     * Example: @MqttMessageHandler("temperatureHandler")
     *
     * @return bean name
     */
    @AliasFor(annotation = Component.class, attribute = "value")
    String value() default "";

    /**
     * Maximum number of concurrent messages to process
     * <p>
     * Only applicable when {@link #ordering()} is enabled (ordering != NONE).
     * Controls the bounded backlog queue capacity of the per-key ordered executor
     * that serializes messages for the same topic/client.
     * 0 = unlimited queue (use the ordered executor without a backlog cap)
     * <p>
     * Default: 0
     *
     * @return maximum concurrent messages
     */
    int maxConcurrentMessages() default 0;

    /**
     * Enable duplicate message detection
     * <p>
     * If true, the handler will attempt to detect and skip duplicate messages
     * based on the packet identifier (for QoS 1/2).
     * <p>
     * Default: false
     *
     * @return true to enable duplicate detection
     */
    boolean deduplicate() default false;

    /**
     * Message processing timeout in milliseconds
     * <p>
     * Maximum time allowed to process a single message.
     * If processing takes longer, it will be interrupted (when async = true).
     * 0 = no timeout
     * <p>
     * Default: 0
     *
     * @return processing timeout in milliseconds
     */
    long timeout() default 0;

    /**
     * Enable error handling for this handler
     * <p>
     * If true, unhandled exceptions will be caught and logged, but processing continues.
     * If false, exceptions will propagate and may stop message processing.
     * <p>
     * Default: true
     *
     * @return true to enable error handling
     */
    boolean errorHandling() default true;

    /**
     * Message ordering guarantee
     * <p>
     * Controls whether messages should be processed in the order they are received.
     * Options:
     * - NONE: No ordering guarantee (default, highest performance)
     * - PER_TOPIC: Messages for the same topic are processed in order
     * - PER_CLIENT: All messages from the same client are processed in order
     * <p>
     * Note: Ordering may reduce throughput, use only when necessary.
     *
     * @return ordering guarantee level
     */
    Ordering ordering() default Ordering.NONE;

    /**
     * Message ordering options
     */
    enum Ordering {
        /**
         * No ordering guarantee - messages processed as they arrive
         */
        NONE,

        /**
         * Messages for the same topic are processed in order
         */
        PER_TOPIC,

        /**
         * All messages from the same client are processed in order
         */
        PER_CLIENT
    }

    /**
     * Handler priority
     * <p>
     * Used when multiple handlers subscribe to the same topic.
     * Higher priority handlers are invoked first.
     * Range: -128 to 127
     * <p>
     * Default: 0
     *
     * @return handler priority
     */
    int priority() default 0;

    /**
     * Enable message validation
     * <p>
     * If true, messages will be validated before processing.
     * Validation includes checking payload format, size limits, etc.
     * <p>
     * Default: false
     *
     * @return true to enable message validation
     */
    boolean validation() default false;

    /**
     * Maximum message payload size in bytes
     * <p>
     * Messages larger than this size will be rejected.
     * 0 = no size limit
     * <p>
     * Default: 0
     *
     * @return maximum payload size in bytes
     */
    int maxPayloadSize() default 0;

    /**
     * Content type for message validation
     * <p>
     * Expected content type of the message payload.
     * Used for validation and automatic deserialization.
     * Examples: "application/json", "text/plain", "application/octet-stream"
     *
     * @return expected content type
     */
    String contentType() default "application/json";

    /**
     * Enable automatic payload deserialization
     * <p>
     * If true and contentType is specified, the payload will be
     * automatically deserialized to the appropriate Java type.
     * <p>
     * Supported content types:
     * - application/json: deserialize to Map or POJO
     * - text/plain: treat as String
     * - application/octet-stream: treat as byte[]
     * <p>
     * Default: false
     *
     * @return true to enable auto-deserialization
     */
    boolean autoDeserialize() default true;

    /**
     * Message handler group
     * <p>
     * Groups multiple handlers together for management purposes.
     * Useful for enabling/disabling groups of handlers.
     *
     * @return handler group name
     */
    String group() default "default";

    /**
     * Enable handler statistics
     * <p>
     * If true, collect statistics about message processing:
     * - Number of messages processed
     * - Processing time
     * - Error count
     * - Throughput
     * <p>
     * Default: false
     *
     * @return true to enable statistics collection
     */
    boolean statistics() default false;

    /**
     * Message handler version
     * <p>
     * Version identifier for the handler.
     * Useful for version management and rolling updates.
     *
     * @return handler version
     */
    String version() default "1.0.0";

    /**
     * Handler description
     * <p>
     * Human-readable description of what this handler does.
     * Used for documentation and monitoring.
     *
     * @return handler description
     */
    String description() default "";

    /**
     * Tags for categorizing handlers
     * <p>
     * Useful for filtering, searching, and organizing handlers.
     * Example: {"sensor", "temperature", "monitoring"}
     *
     * @return array of tags
     */
    String[] tags() default {};
}