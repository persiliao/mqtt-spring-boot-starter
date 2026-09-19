package io.github.persiliao.mqtt;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.github.persiliao.mqtt.PayloadConstants.RECOMMENDED_MAX_PACKET_SIZE;

/**
 * MQTT Auto-configuration Properties
 * <p>
 * This class defines all configuration properties for the MQTT client auto-configuration.
 * It supports both single server and multiple server modes with comprehensive configuration options.
 * <p>
 * Validation is performed through the explicit {@link #validate()} and
 * {@link ServerConfig#isValid()} methods rather than the Bean Validation API, so that this
 * starter remains independent of the {@code jakarta.*} / {@code javax.*} namespace split
 * and is therefore compatible with both Spring Boot 2.x and 3.x.
 * <p>
 * Configuration prefix: "mqtt"
 * Example: mqtt.enabled=true, mqtt.mode=SINGLE, mqtt.single-server.server-uri=tcp://localhost:1883
 */
@Data
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
    private boolean enabled = true;

    /**
     * MQTT client operation mode
     * - SINGLE: Connect to a single MQTT server
     * - MULTI: Connect to multiple MQTT servers
     * Default: SINGLE
     */
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
    public static class ServerConfig {
        private String id;

        private String serverUri;

        private String clientId;

        private String username;
        private String password;

        private int initialDelay = 1;

        private int maxDelay = 30;

        private int keepAlive = 60;

        private int connectionTimeout = 10;

        private int sessionExpiryInterval = 3600;

        private boolean automaticReconnect = true;
        private boolean cleanStart = false;

        private int receiveMaximum = 32;

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
         */
        public int getPort() {
            URI uri = parseUri();
            int port = uri.getPort();

            if (port > 0) {
                return port;
            }

            // Return default port based on scheme
            String scheme = uri.getScheme();
            if (scheme == null) {
                return 1883;
            }
            switch (scheme.toLowerCase()) {
                case "ssl":
                case "wss":
                    return 8883;
                case "ws":
                    return 80;
                case "tcp":
                    return 1883;
                default:
                    return 1883;
            }
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
            return "ServerConfig{" +
                    "id='" + (id != null ? id : "default") + "', " +
                    "uri='" + serverUri + "', " +
                    "clientId='" + clientId + "', " +
                    "host='" + getHost() + ":" + getPort() + "', " +
                    "protocol='" + getProtocol() + "'" +
                    "}";
        }
    }

    /**
     * Multiple Servers Configuration
     */
    @Data
    public static class MultiServerConfig {
        private boolean failFast;

        private List<ServerConfig> servers;

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
                    throw new IllegalArgumentException(String.format("Server configuration at index %d is null", i));
                }

                if (!config.isValid()) {
                    throw new IllegalArgumentException(String.format("Invalid server configuration at index %d: %s", i, config));
                }

                // In MULTI mode, each server must have a unique ID
                if (config.getId() == null || config.getId().trim().isEmpty()) {
                    throw new IllegalArgumentException(String.format("Server configuration at index %d must have an ID in MULTI mode", i));
                }
            }

            List<String> allIds = servers.stream().map(ServerConfig::getId).collect(Collectors.toList());

            Set<String> uniqueIds = new HashSet<>(allIds);
            if (uniqueIds.size() != allIds.size()) {
                throw new IllegalArgumentException("Duplicate server IDs found in MULTI mode configuration");
            }
        }

        /**
         * Returns a server configuration by ID
         */
        public Optional<ServerConfig> getServerById(String id) {
            if (id == null || id.trim().isEmpty() || servers == null || servers.isEmpty()) {
                return Optional.empty();
            }

            return servers.stream().filter(server -> id.equals(server.getId())).findFirst();
        }

        /**
         * Returns server configurations by predicate
         */
        public List<ServerConfig> getServersBy(Predicate<ServerConfig> predicate) {
            Objects.requireNonNull(predicate, "Predicate cannot be null");

            return servers.stream().filter(predicate).collect(Collectors.toList());
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
            return servers == null ? 0 : servers.size();
        }

        /**
         * Checks if the configuration contains any servers
         */
        public boolean hasServers() {
            return servers != null && !servers.isEmpty();
        }

        /**
         * Returns a string representation for logging
         */
        @Override
        public String toString() {
            String serverList = servers == null ? "" : servers.stream().map(ServerConfig::toString).collect(Collectors.joining(", "));
            return "MultiServerConfig{" +
                    "failFast=" + failFast + ", " +
                    "serverCount=" + getServerCount() + ", " +
                    "servers=" + serverList +
                    "}";
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
            case SINGLE:
                Objects.requireNonNull(singleServer, "Single server configuration cannot be null in SINGLE mode");
                if (!singleServer.isValid()) {
                    throw new IllegalArgumentException("Invalid single server configuration: " + singleServer);
                }
                break;
            case MULTI:
                Objects.requireNonNull(multiServer, "Multi server configuration cannot be null in MULTI mode");
                multiServer.validate();
                break;
            default:
                break;
        }
    }

    /**
     * Returns the default server configuration based on mode
     */
    public ServerConfig getDefaultServerConfig() {
        validate();

        if (mode == Mode.SINGLE) {
            return singleServer;
        }
        if (!multiServer.hasServers()) {
            throw new IllegalStateException("No servers configured in MULTI mode");
        }
        return multiServer.servers.get(0);
    }

    /**
     * Returns all server configurations
     */
    public List<ServerConfig> getAllServerConfigs() {
        List<ServerConfig> configs = new ArrayList<>();
        if (mode == Mode.SINGLE) {
            if (singleServer != null && singleServer.isValid()) {
                configs.add(singleServer);
            }
        } else {
            if (multiServer != null && multiServer.hasServers()) {
                configs.addAll(multiServer.getServers());
            }
        }
        return configs;
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

        return "MQTT Configuration Summary:\n" +
                "==========================\n" +
                "Enabled: " + enabled + "\n" +
                "Mode: " + mode + "\n" +
                "Total Servers: " + totalServers + "\n" +
                "Servers with Authentication: " + authServers + "\n" +
                "Protocols: " + getServersByProtocol().keySet() + "\n";
    }
}
