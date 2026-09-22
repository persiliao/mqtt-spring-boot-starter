package io.github.persiliao.mqtt;

import io.github.persiliao.mqtt.handler.MqttSubscriptionManager;
import org.springframework.core.annotation.AliasFor;
import org.springframework.stereotype.Component;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a Spring bean as an MQTT message handler.
 *
 * <p>The annotated bean is discovered once the application context is refreshed.
 * For every configured server the topics listed in {@link #topics()} are
 * subscribed automatically, and every incoming publish is routed to the bean's
 * <em>handler methods</em>.
 *
 * <p><b>Handler methods</b> are public, non-static instance methods whose name
 * starts with {@code handle} and which declare at least one resolvable
 * parameter, e.g. {@code handleMessage(String topic, String payload)}.
 * When several handler methods exist they are tried in declaration order and
 * the first one that completes without an exception wins.
 *
 * <p>Supported parameter types:
 * <ul>
 *   <li>{@code String} — the topic (first parameter) or the payload decoded as UTF-8</li>
 *   <li>{@code byte[]} — the raw payload</li>
 *   <li>{@code Mqtt5Publish} — the full MQTT 5 publish message</li>
 *   <li>{@link MqttMessageContext} — envelope with topic, payload, server id and message</li>
 *   <li>{@code Map} or any POJO — the payload deserialized from JSON
 *       (requires {@link #autoDeserialize()} to be {@code true})</li>
 * </ul>
 *
 * <p>Usage:
 * <pre>{@code
 * @MqttMessageHandler(topics = "sensor/temperature")
 * public class TemperatureHandler {
 *     public void handleMessage(String topic, String payload) {
 *         // process the message
 *     }
 * }
 * }</pre>
 *
 * @see MqttMessageContext
 * @since 3.0.0
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface MqttMessageHandler {

    /**
     * MQTT topic filters to subscribe to.
     *
     * <p>Supports the standard MQTT wildcards: {@code +} (single level) and
     * {@code #} (multi level), e.g. {@code "home/+/temperature"} or
     * {@code "home/#"}.
     *
     * @return the topic filters (at least one required)
     */
    String[] topics() default {};

    /**
     * Quality of Service used for the subscriptions.
     *
     * @return QoS 0, 1 or 2; defaults to 1 (at least once)
     */
    int qos() default 1;

    /**
     * Whether messages are processed asynchronously.
     *
     * <p>If {@code true} (the default) handler methods run on the dedicated
     * async pool configured via {@code mqtt.async.*}, keeping the MQTT client
     * thread free. If {@code false} the handler runs synchronously on the
     * MQTT client callback thread; long-running handlers then stall message
     * ingestion for that connection.
     *
     * <p>Ignored when {@link #ordering()} is not {@link Ordering#NONE} —
     * ordered processing always runs off the MQTT thread.
     *
     * @return {@code true} to process messages asynchronously
     */
    boolean async() default true;

    /**
     * Server id this handler subscribes to.
     *
     * <p>Empty (the default) means "all configured servers". In {@code MULTI}
     * mode the id must match a {@code mqtt.multi-server.servers[n].id}; the
     * special value {@code "*"} is equivalent to the empty value.
     *
     * @return a single server id
     */
    String serverId() default "";

    /**
     * Multiple server ids this handler subscribes to.
     *
     * <p>Takes precedence over {@link #serverId()} when non-empty.
     *
     * @return the server ids
     */
    String[] serverIds() default {};

    /**
     * MQTT 5 "retain as published" flag for the subscriptions.
     *
     * <p>If {@code true} the retain flag of a received message reflects the
     * value used at publish time, letting the handler distinguish retained
     * messages from fresh ones.
     *
     * @return {@code true} to preserve the published retain flag
     */
    boolean retainAsPublished() default false;

    /**
     * Whether to drop messages whose content fingerprint matches a recently
     * processed message on the same subscription.
     *
     * <p>Deduplication is content-based (topic, QoS, retain flag and payload)
     * and keeps a bounded, least-recently-used window per subscription. It
     * mitigates QoS 1/2 re-delivery but does NOT make the handler idempotent —
     * two legitimately identical messages published close together will be
     * deduplicated as well.
     *
     * @return {@code true} to enable duplicate suppression
     */
    boolean deduplicate() default false;

    /**
     * Ordering guarantee for messages handled by this handler.
     *
     * <ul>
     *   <li>{@link Ordering#NONE} — no guarantee, highest throughput</li>
     *   <li>{@link Ordering#PER_TOPIC} — same topic processed in order</li>
     *   <li>{@link Ordering#PER_CLIENT} — all messages of the same server
     *       connection processed in order</li>
     * </ul>
     *
     * @return the ordering guarantee
     */
    Ordering ordering() default Ordering.NONE;

    /**
     * Bounded backlog queue capacity of the ordered executor, only relevant
     * when {@link #ordering()} is not {@link Ordering#NONE}.
     *
     * <p>{@code 0} (the default) falls back to a built-in default. When the
     * backlog is full the MQTT client thread runs the handler itself as
     * backpressure (caller-runs), so messages are never dropped.
     *
     * @return the backlog queue capacity
     */
    int maxConcurrentMessages() default 0;

    /**
     * Whether JSON payloads are deserialized into the declared parameter type
     * ({@code Map} or POJO).
     *
     * @return {@code true} to auto-deserialize JSON payloads
     */
    boolean autoDeserialize() default true;

    /**
     * Declared content type of the payload.
     *
     * <p>Only used to decide how to deserialize: {@code application/json}
     * triggers JSON deserialization for {@code Map}/POJO parameters.
     *
     * @return the expected content type
     */
    String contentType() default "application/json";

    /**
     * Maximum accepted payload size in bytes. Messages larger than this are
     * dropped (and counted as failures in the statistics).
     *
     * @return the maximum payload size, {@code 0} for no limit
     */
    int maxPayloadSize() default 0;

    /**
     * Whether per-handler processing statistics are collected and exposed via
     * {@link MqttSubscriptionManager#getStatistics()}.
     *
     * @return {@code true} to collect statistics
     */
    boolean statistics() default true;

    /**
     * Logical group this handler belongs to (metadata only, useful for
     * documentation and tooling).
     *
     * @return the group name
     */
    String group() default "default";

    /**
     * Human-readable description of the handler (metadata only).
     *
     * @return the description
     */
    String description() default "";

    /**
     * Free-form tags for categorizing the handler (metadata only).
     *
     * @return the tags
     */
    String[] tags() default {};

    /**
     * Alias for {@link Component#value()} — the bean name of the handler.
     *
     * @return the bean name
     */
    @AliasFor(annotation = Component.class, attribute = "value")
    String value() default "";

    /**
     * Ordering guarantees supported by {@link #ordering()}.
     */
    enum Ordering {
        /** No ordering guarantee; messages may be processed concurrently. */
        NONE,
        /** Messages of the same topic are processed in receive order. */
        PER_TOPIC,
        /** All messages of the same server connection are processed in receive order. */
        PER_CLIENT
    }
}
