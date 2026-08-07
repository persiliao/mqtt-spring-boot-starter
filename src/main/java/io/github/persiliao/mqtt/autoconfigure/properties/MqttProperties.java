package io.github.persiliao.mqtt.autoconfigure.properties;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.github.persiliao.mqtt.autoconfigure.PayloadConstants.RECOMMENDED_MAX_PACKET_SIZE;

/**
 * MQTT Auto-configuration Properties
 * <p>
 * This class defines all configuration properties for the MQTT client auto-configuration.
 * It supports both single server and multiple server modes with comprehensive configuration options.
 * <p>
 * 1. Records for immutable configuration
 * 2. Sealed classes for type safety
 * 3. Text blocks for formatted strings
 * 4. Pattern matching for instanceof
 * 5. Enhanced switch expressions
 * <p>
 * Configuration prefix: "mqtt"
 * Example: mqtt.enabled=true, mqtt.mode=SINGLE, mqtt.single-server.server-uri=tcp://localhost:1883
 */
@Data
@Validated
@ConfigurationProperties(prefix = "mqtt")
public class MqttProperties {

    /**
     * MQTT operation mode enumeration
     */
    public enum Mode {
        SINGLE, MULTI
    }

    /**
     * Enable or disable MQTT auto-configuration
     * Default: true (enabled)
     */
    @NotNull
    private boolean enabled = true;

    /**
     * MQTT client operation mode
     * - SINGLE: Connect to a single MQTT server
     * - MULTI: Connect to multiple MQTT servers
     * Default: SINGLE
     */
    @NotNull
    private Mode mode = Mode.SINGLE;

    /**
     * Configuration for single server mode
     * Used when mode = SINGLE
     */
    @NestedConfigurationProperty
    private ServerConfig singleServer;

    /**
     * Configuration for multiple servers mode
     * Used when mode = MULTI
     */
    @NestedConfigurationProperty
    private MultiServerConfig multiServer;

    /**
     * Configuration for the asynchronous message-processing executor.
     * Used when a handler is declared with async = true (the default).
     */
    @NestedConfigurationProperty
    private AsyncConfig async = new AsyncConfig();

    /**
     * Immutable server configuration record
     */
    @Data
    @Validated
    public static class ServerConfig {
        private String id;

        @NotBlank(message = "MQTT server URI cannot be blank")
        private String serverUri;

        @NotBlank(message = "MQTT client ID cannot be blank")
        private String clientId;

        private String username;
        private String password;

        @Min(value = 1, message = "Initial delay must be at least 1 second")
        private int initialDelay = 1;

        @Min(value = 1, message = "Maximum delay must be at least 1 second")
        private int maxDelay = 30;

        @Min(value = 0, message = "Keep alive must be 0 or positive")
        private int keepAlive = 60;

        @Positive(message = "Connection timeout must be positive")
        private int connectionTimeout = 10;

        @Min(value = 0, message = "Session expiry interval must be 0 or positive")
        private int sessionExpiryInterval = 3600;

        private boolean automaticReconnect = true;
        private boolean cleanStart = false;

        @Min(value = 1, message = "Receive maximum must be at least 1")
        private int receiveMaximum = 32;

        @Min(value = 1024, message = "Maximum packet size must be at least 1024 bytes")
        private int maximumPacketSize = RECOMMENDED_MAX_PACKET_SIZE;

        /**
         * Parse URI using Java's built-in URI class
         */
        public URI parseUri() {
            try {
                return new URI(serverUri);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid MQTT URI: " + serverUri, e);
            }
        }

        /**
         * Returns the server host extracted from the URI
         */
        public String getHost() {
            URI uri = parseUri();
            String host = uri.getHost();
            return host != null ? host : "localhost";
        }

        /**
         * Returns the server port extracted from the URI
         * Using enhanced switch expression
         */
        public int getPort() {
            URI uri = parseUri();
            int port = uri.getPort();

            if (port > 0) {
                return port;
            }

            // Return default port based on scheme
            return switch (uri.getScheme().toLowerCase()) {
                case "ssl", "wss" -> 8883;
                case "ws" -> 80;
                case "tcp" -> 1883;
                default -> 1883;
            };
        }

        /**
         * Returns the protocol from the URI
         */
        public String getProtocol() {
            URI uri = parseUri();
            return uri.getScheme() != null ? uri.getScheme() : "tcp";
        }

        /**
         * Validates if authentication is configured
         */
        public boolean hasAuthentication() {
            return StringUtils.hasText(username) && StringUtils.hasText(password);
        }

        /**
         * Validates the configuration
         */
        public boolean isValid() {
            return StringUtils.hasText(clientId) && StringUtils.hasText(serverUri);
        }

        /**
         * Returns a string representation for logging
         */
        @Override
        public String toString() {
            return """
                    ServerConfig{
                        id='%s', 
                        uri='%s', 
                        clientId='%s', 
                        host='%s:%d',
                        protocol='%s'
                    }""".formatted(id != null ? id : "default", serverUri, clientId, getHost(), getPort(), getProtocol());
        }
    }

    /**
     * Multiple Servers Configuration
     */
    @Data
    @Validated
    public static class MultiServerConfig {
        private boolean failFast;

        @NotNull(message = "Server list cannot be null")
        private List<@NotNull ServerConfig> servers;

        /**
         * Validates the multi-server configuration
         */
        public void validate() {
            Objects.requireNonNull(servers, "Server list cannot be null");

            if (servers.isEmpty()) {
                throw new IllegalArgumentException("No servers configured for MULTI mode");
            }

            // Check for null configurations
            for (int i = 0; i < servers.size(); i++) {
                ServerConfig config = servers.get(i);
                if (config == null) {
                    throw new IllegalArgumentException("Server configuration at index %d is null".formatted(i));
                }

                if (!config.isValid()) {
                    throw new IllegalArgumentException("Invalid server configuration at index %d: %s".formatted(i, config));
                }

                // In MULTI mode, each server must have a unique ID
                if (config.getId() == null || config.getId().isBlank()) {
                    throw new IllegalArgumentException("Server configuration at index %d must have an ID in MULTI mode".formatted(i));
                }
            }

            List<String> allIds = servers.stream().map(ServerConfig::getId).toList();

            Set<String> uniqueIds = new HashSet<>(allIds);
            if (uniqueIds.size() != allIds.size()) {
                throw new IllegalArgumentException("Duplicate server IDs found in MULTI mode configuration");
            }
        }

        /**
         * Returns a server configuration by ID
         */
        public Optional<ServerConfig> getServerById(String id) {
            if (id == null || id.isBlank() || servers.isEmpty()) {
                return Optional.empty();
            }

            return servers.stream().filter(server -> id.equals(server.getId())).findFirst();
        }

        /**
         * Returns server configurations by predicate
         */
        public List<ServerConfig> getServersBy(Predicate<ServerConfig> predicate) {
            Objects.requireNonNull(predicate, "Predicate cannot be null");

            return servers.stream().filter(predicate).toList();
        }

        /**
         * Returns server configurations that require authentication
         */
        public List<ServerConfig> getServersWithAuth() {
            return getServersBy(ServerConfig::hasAuthentication);
        }

        /**
         * Returns server configurations with automatic reconnection enabled
         */
        public List<ServerConfig> getServersWithReconnect() {
            return getServersBy(ServerConfig::isAutomaticReconnect);
        }

        /**
         * Returns the number of configured servers
         */
        public int getServerCount() {
            return servers.size();
        }

        /**
         * Checks if the configuration contains any servers
         */
        public boolean hasServers() {
            return !servers.isEmpty();
        }

        /**
         * Returns a string representation for logging
         */
        @Override
        public String toString() {
            return """
                    MultiServerConfig{
                        failFast=%s, 
                        serverCount=%d,
                        servers=%s
                    }""".formatted(failFast, getServerCount(), servers.stream().map(ServerConfig::toString).collect(Collectors.joining(", ")));
        }
    }

    /**
     * Configuration for the asynchronous message-processing thread pool.
     * <p>
     * Messages are processed off the MQTT client thread on this executor when a
     * handler is declared with {@code async = true}. A value of 0 falls back to a
     * sensible default so the starter works out of the box.
     */
    @Data
    public static class AsyncConfig {
        /**
         * Core pool size of the async executor.
         * 0 = fall back to the number of available processors.
         */
        private int corePoolSize = 0;

        /**
         * Maximum pool size of the async executor.
         * 0 = fall back to (available processors * 2).
         */
        private int maxPoolSize = 0;

        /**
         * Bounded queue capacity before the executor's rejection policy applies.
         * 0 = fall back to 1024.
         */
        private int queueCapacity = 0;
    }

    /**
     * Validates the entire MQTT configuration
     */
    public void validate() {
        if (!enabled) {
            return; // Skip validation if MQTT is disabled
        }

        Objects.requireNonNull(mode, "MQTT mode cannot be null");

        switch (mode) {
            case SINGLE -> {
                Objects.requireNonNull(singleServer, "Single server configuration cannot be null in SINGLE mode");
                if (!singleServer.isValid()) {
                    throw new IllegalArgumentException("Invalid single server configuration: " + singleServer);
                }
            }
            case MULTI -> {
                Objects.requireNonNull(multiServer, "Multi server configuration cannot be null in MULTI mode");
                multiServer.validate();
            }
        }
    }

    /**
     * Returns the default server configuration based on mode
     */
    public ServerConfig getDefaultServerConfig() {
        validate();

        return switch (mode) {
            case SINGLE -> singleServer;
            case MULTI -> {
                if (!multiServer.hasServers()) {
                    throw new IllegalStateException("No servers configured in MULTI mode");
                }
                yield multiServer.servers.get(0);
            }
        };
    }

    /**
     * Returns all server configurations
     */
    public List<ServerConfig> getAllServerConfigs() {
        return switch (mode) {
            case SINGLE -> {
                List<ServerConfig> configs = new ArrayList<>();
                if (singleServer != null && singleServer.isValid()) {
                    configs.add(singleServer);
                }
                yield configs;
            }
            case MULTI -> {
                List<ServerConfig> configs = new ArrayList<>();
                if (multiServer != null && multiServer.hasServers()) {
                    configs.addAll(multiServer.getServers());
                }
                yield configs;
            }
        };
    }

    /**
     * Returns server configurations by protocol
     */
    public Map<String, List<ServerConfig>> getServersByProtocol() {
        return getAllServerConfigs().stream().collect(Collectors.groupingBy(ServerConfig::getProtocol, Collectors.toList()));
    }

    /**
     * Returns unique client IDs
     */
    public Set<String> getUniqueClientIds() {
        return getAllServerConfigs().stream().map(ServerConfig::getClientId).collect(Collectors.toSet());
    }

    /**
     * Checks if any server requires authentication
     */
    public boolean hasServersRequiringAuth() {
        return getAllServerConfigs().stream().anyMatch(ServerConfig::hasAuthentication);
    }

    /**
     * Returns a summary of the configuration
     */
    public String getSummary() {
        int totalServers = getAllServerConfigs().size();
        int authServers = (int) getAllServerConfigs().stream().filter(ServerConfig::hasAuthentication).count();

        return """
                MQTT Configuration Summary:
                ==========================
                Enabled: %s
                Mode: %s
                Total Servers: %d
                Servers with Authentication: %d
                Protocols: %s
                """.formatted(enabled, mode, totalServers, authServers, getServersByProtocol().keySet());
    }
}
