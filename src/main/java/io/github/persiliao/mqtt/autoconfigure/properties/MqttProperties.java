package io.github.persiliao.mqtt.autoconfigure.properties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Configuration properties for the MQTT auto-configuration.
 *
 * <p>Configuration prefix: {@code mqtt}. Example:
 * <pre>
 * mqtt:
 *   mode: SINGLE
 *   single-server:
 *     server-uri: tcp://localhost:1883
 *     client-id: my-app
 * </pre>
 *
 * <p>Validation is performed explicitly through {@link #validate()} (called by
 * the auto-configuration at startup) rather than via the Bean Validation API,
 * keeping the starter free of any {@code jakarta.validation} dependency.
 *
 * @since 3.0.0
 * @author Persi.Liao <xiangchu.liao@gmail.com>
 */
@Getter
@Setter
@ConfigurationProperties(prefix = "mqtt")
public class MqttProperties {

    /**
     * MQTT client operation mode.
     */
    public enum Mode {
        /** Connect to a single MQTT server ({@code mqtt.single-server.*}). */
        SINGLE,
        /** Connect to multiple MQTT servers ({@code mqtt.multi-server.servers}). */
        MULTI
    }

    /**
     * Enables the MQTT auto-configuration.
     * Default: {@code true}
     */
    private boolean enabled = true;

    /**
     * Operation mode.
     * Default: {@link Mode#SINGLE}
     */
    private Mode mode = Mode.SINGLE;

    /**
     * Single server configuration, used when {@link #mode} is {@code SINGLE}.
     */
    @NestedConfigurationProperty
    private ServerConfig singleServer;

    /**
     * Multiple server configuration, used when {@link #mode} is {@code MULTI}.
     */
    @NestedConfigurationProperty
    private MultiServerConfig multiServer;

    /**
     * Pool sizing for the async message-processing executor used by handlers
     * declared with {@code async = true}.
     */
    @NestedConfigurationProperty
    private AsyncConfig async = new AsyncConfig();

    /**
     * Configuration of one MQTT server connection.
     */
    @Getter
    @Setter
    @ToString
    public static class ServerConfig {

        /**
         * Logical id of this server. Required in {@code MULTI} mode (it is the
         * key of the client map and the value passed to handlers as server id).
         * Optional in {@code SINGLE} mode, defaults to {@code "default"}.
         */
        private String id;

        /**
         * Broker address. Supported schemes: {@code tcp://} and {@code ssl://}
         * (TLS). A missing scheme defaults to {@code tcp}.
         * Examples: {@code tcp://localhost:1883}, {@code ssl://broker.example.com:8883}.
         */
        private String serverUri;

        /**
         * MQTT client identifier. Must be unique per broker.
         */
        private String clientId;

        /**
         * Username for MQTT authentication (used together with {@link #password}).
         */
        private String username;

        /**
         * Password for MQTT authentication. Never included in
         * {@link #toString()} to avoid leaking credentials into logs.
         */
        @ToString.Exclude
        private String password;

        /**
         * MQTT keep-alive interval in seconds (0–65535).
         * Default: 60
         */
        private int keepAlive = 60;

        /**
         * Session expiry interval in seconds (MQTT 5).
         * Default: 3600
         */
        private long sessionExpiryInterval = 3600;

        /**
         * MQTT 5 "clean start" flag.
         * Default: {@code true}
         *
         * <p>Defaults to a clean start because a fresh client process cannot consume a
         * resumed session: the broker immediately flushes QoS 1 messages queued in the
         * previous session, but this process has not registered any publish handlers yet,
         * so the client drops them (logging "No publish flow registered"). A clean start
         * discards the stale session instead, making every startup deterministic.
         * Set to {@code false} to keep the broker session across reconnects within one
         * process (queued messages are then redelivered on automatic reconnect).
         */
        private boolean cleanStart = true;

        /**
         * Enables the client's built-in automatic reconnection with exponential
         * backoff between {@link #initialDelay} and {@link #maxDelay}.
         * Default: {@code true}
         */
        private boolean automaticReconnect = true;

        /**
         * Initial delay of the reconnection backoff.
         * Default: 1s
         */
        private Duration initialDelay = Duration.ofSeconds(1);

        /**
         * Upper bound of the reconnection backoff.
         * Default: 30s
         */
        private Duration maxDelay = Duration.ofSeconds(30);

        /**
         * MQTT 5 receive maximum (1–65535).
         * Default: 32
         */
        private int receiveMaximum = 32;

        /**
         * MQTT 5 maximum packet size in bytes.
         * Default: 8388608 (8 MiB)
         */
        private int maximumPacketSize = 8 * 1024 * 1024;

        /**
         * Returns the effective id of this server: the configured {@link #id}
         * or {@code "default"} when it is not set.
         *
         * @return the server id
         */
        public String resolveId() {
            return StringUtils.hasText(id) ? id.trim() : "default";
        }

        /**
         * @return {@code true} when both username and password are set
         */
        public boolean hasAuthentication() {
            return StringUtils.hasText(username) && StringUtils.hasText(password);
        }

        /**
         * Validates this server configuration, rejecting values the broker
         * would refuse at connect time (out-of-range keep-alive, receive
         * maximum, packet size, ...) and unusable reconnect delays.
         *
         * <p>Failing fast here turns an opaque runtime exception from the
         * MQTT stack into a readable startup error.
         *
         * @param path the configuration path used in error messages,
         *             e.g. {@code mqtt.single-server}
         * @throws IllegalArgumentException if any value is invalid
         */
        public void validate(String path) {
            if (!StringUtils.hasText(serverUri)) {
                throw new IllegalArgumentException(path + ".server-uri is required");
            }
            if (!StringUtils.hasText(clientId)) {
                throw new IllegalArgumentException(path + ".client-id is required");
            }
            if (keepAlive < 0 || keepAlive > 65535) {
                throw new IllegalArgumentException(
                        path + ".keep-alive must be between 0 and 65535 but was " + keepAlive);
            }
            if (sessionExpiryInterval < 0 || sessionExpiryInterval > 4294967295L) {
                throw new IllegalArgumentException(
                        path + ".session-expiry-interval must be between 0 and 4294967295 but was "
                                + sessionExpiryInterval);
            }
            if (receiveMaximum < 1 || receiveMaximum > 65535) {
                throw new IllegalArgumentException(
                        path + ".receive-maximum must be between 1 and 65535 but was " + receiveMaximum);
            }
            if (maximumPacketSize <= 0) {
                throw new IllegalArgumentException(
                        path + ".maximum-packet-size must be positive but was " + maximumPacketSize);
            }
            if (automaticReconnect) {
                if (initialDelay == null || initialDelay.isNegative()) {
                    throw new IllegalArgumentException(
                            path + ".initial-delay must be a non-negative duration but was " + initialDelay);
                }
                if (maxDelay == null || maxDelay.isNegative()) {
                    throw new IllegalArgumentException(
                            path + ".max-delay must be a non-negative duration but was " + maxDelay);
                }
                if (initialDelay.compareTo(maxDelay) > 0) {
                    throw new IllegalArgumentException(path + ".initial-delay (" + initialDelay
                            + ") must not exceed max-delay (" + maxDelay + ")");
                }
            }
        }
    }

    /**
     * Multiple server configuration for {@link Mode#MULTI}.
     */
    @Getter
    @Setter
    @ToString
    public static class MultiServerConfig {

        /**
         * When {@code true} the application fails to start if any server
         * cannot be created. Default: {@code false} (the failing server is
         * logged and skipped).
         */
        private boolean failFast;

        /**
         * The configured servers.
         */
        private List<ServerConfig> servers;

        /**
         * Validates the multi-server configuration.
         *
         * @throws IllegalArgumentException if the configuration is invalid
         */
        public void validate() {
            if (servers == null || servers.isEmpty()) {
                throw new IllegalArgumentException(
                        "mqtt.multi-server.servers must contain at least one server in MULTI mode");
            }
            Set<String> seen = new HashSet<>();
            for (int i = 0; i < servers.size(); i++) {
                ServerConfig config = servers.get(i);
                if (config == null) {
                    throw new IllegalArgumentException("mqtt.multi-server.servers[" + i + "] is null");
                }
                if (!StringUtils.hasText(config.getId())) {
                    throw new IllegalArgumentException(
                            "mqtt.multi-server.servers[" + i + "].id is required in MULTI mode");
                }
                // Dedupe on the *resolved* id: "a" and " a " would otherwise be
                // accepted as two distinct servers yet register under the same key.
                if (!seen.add(config.resolveId())) {
                    throw new IllegalArgumentException(
                            "Duplicate server id '" + config.resolveId() + "' in mqtt.multi-server.servers");
                }
                config.validate("mqtt.multi-server.servers[" + i + "]");
            }
        }
    }

    /**
     * Pool sizing for the async message-processing executor.
     *
     * <p>A value of {@code 0} (the default) falls back to a sensible
     * default: core = available processors, max = 2x available processors,
     * queue capacity = 1024.
     */
    @Getter
    @Setter
    @ToString
    public static class AsyncConfig {

        /**
         * Core pool size; {@code 0} = available processors.
         */
        private int corePoolSize;

        /**
         * Maximum pool size; {@code 0} = 2x available processors.
         */
        private int maxPoolSize;

        /**
         * Bounded queue capacity before backpressure (caller-runs) applies;
         * {@code 0} = 1024.
         */
        private int queueCapacity;

        /**
         * Maximum number of per-key ordered executors created for handlers
         * declared with an {@code ordering} other than {@code NONE}.
         *
         * <p>The ordering key is derived from the topic of the incoming
         * message, so a wildcard subscription such as {@code sensor/#} can
         * produce an unbounded number of keys. This cap keeps the thread count
         * bounded; keys beyond the cap share the async pool instead.
         * Default: {@code 64}
         */
        private int maxOrderedExecutors = 64;
    }

    /**
     * Validates the whole configuration. Called by the auto-configuration
     * before any client is created, so invalid settings fail fast at startup.
     *
     * @throws IllegalArgumentException if the configuration is invalid
     */
    public void validate() {
        if (!enabled) {
            return;
        }
        Objects.requireNonNull(mode, "mqtt.mode cannot be null");
        switch (mode) {
            case SINGLE -> {
                if (singleServer == null) {
                    throw new IllegalArgumentException(
                            "mqtt.single-server must be configured in SINGLE mode (server-uri and client-id are required)");
                }
                singleServer.validate("mqtt.single-server");
            }
            case MULTI -> {
                if (multiServer == null) {
                    throw new IllegalArgumentException(
                            "mqtt.multi-server must be configured in MULTI mode");
                }
                multiServer.validate();
            }
        }
    }

    @Override
    public String toString() {
        return "MqttProperties{enabled=" + enabled + ", mode=" + mode
                + ", singleServer=" + singleServer
                + ", multiServer=" + multiServer
                + ", async=" + async + "}";
    }
}
