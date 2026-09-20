package io.github.persiliao.mqtt.autoconfigure;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import io.github.persiliao.mqtt.constant.BeanConstants;
import io.github.persiliao.mqtt.hander.MqttMessageHandlerProcessor;
import io.github.persiliao.mqtt.autoconfigure.properties.MqttProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Configuration
@RequiredArgsConstructor
@EnableConfigurationProperties(MqttProperties.class)
@ConditionalOnProperty(prefix = "mqtt", name = "enabled", havingValue = "true", matchIfMissing = true)
public class MqttAutoConfiguration {

    private final MqttProperties mqttProperties;

    // Use ConcurrentHashMap to track connection status for logging purposes
    private final Map<String, Boolean> connectionStatus = new ConcurrentHashMap<>();

    /**
     * Creates a single MQTT client bean for SINGLE mode.
     * This bean is created only when:
     * 1. No bean named "singleMqttClient" exists
     * 2. Property mqtt.mode is SINGLE or not specified (defaults to SINGLE)
     *
     * @return Mqtt5AsyncClient instance
     */
    @Bean
    @ConditionalOnMissingBean(name = BeanConstants.SINGLE_MQTT_CLIENT)
    @ConditionalOnProperty(prefix = "mqtt", name = "mode", havingValue = "SINGLE", matchIfMissing = true)
    public Mqtt5AsyncClient singleMqttClient() {
        MqttProperties.ServerConfig config = mqttProperties.getSingleServer();
        validateServerConfig(config, "single");
        return createAndConnectMqttClient(config);
    }

    /**
     * Creates multiple MQTT client beans for MULTI mode.
     * This bean is created only when:
     * 1. No bean named "multiMqttClients" exists
     * 2. Property mqtt.mode is MULTI
     *
     * @return Map of server ID to Mqtt5AsyncClient instances
     */
    @Bean
    @ConditionalOnMissingBean(name = BeanConstants.MULTI_MQTT_CLIENTS)
    @ConditionalOnProperty(prefix = "mqtt", name = "mode", havingValue = "MULTI")
    public Map<String, Mqtt5AsyncClient> multiMqttClients() {
        Map<String, Mqtt5AsyncClient> clients = new ConcurrentHashMap<>();
        List<MqttProperties.ServerConfig> servers = mqttProperties.getMultiServer().getServers();

        for (MqttProperties.ServerConfig config : servers) {
            try {
                validateServerConfig(config, "multi");
                Mqtt5AsyncClient client = createAndConnectMqttClient(config);
                clients.put(config.getId(), client);
            } catch (Exception e) {
                log.error("Failed to create MQTT client for server config: {}", config, e);
                if (mqttProperties.getMultiServer().isFailFast()) {
                    throw new IllegalStateException("Failed to create MQTT client for config: " + config, e);
                }
            }
        }

        return clients;
    }

    @Bean
    @ConditionalOnMissingBean
    public static MqttMessageHandlerProcessor mqttMessageHandlerProcessor() {
        return new MqttMessageHandlerProcessor();
    }

    /**
     * Validates the server configuration.
     *
     * @param config Server configuration to validate
     * @param mode   Operation mode ("single" or "multi")
     * @throws IllegalArgumentException if configuration is invalid
     */
    private void validateServerConfig(MqttProperties.ServerConfig config, String mode) {
        if (config == null) {
            throw new IllegalArgumentException("MQTT server config cannot be null for mode: " + mode);
        }

        if (!StringUtils.hasText(config.getServerUri())) {
            throw new IllegalArgumentException("MQTT server URI cannot be empty for mode: " + mode);
        }

        if (!StringUtils.hasText(config.getClientId())) {
            throw new IllegalArgumentException("MQTT clientId cannot be empty for mode: " + mode + ". Please configure the mqtt." + mode + ".client-id property.");
        }
    }

    /**
     * Creates and connects an MQTT client with the given configuration.
     *
     * @param config Server configuration
     * @return Connected MQTT client
     */
    private Mqtt5AsyncClient createAndConnectMqttClient(MqttProperties.ServerConfig config) {
        Mqtt5AsyncClient client = buildMqtt5AsyncClient(config);
        String serverId = getServerId(config);
        connectClient(client, config, serverId);
        return client;
    }

    /**
     * Gets the server ID from configuration.
     * Uses client ID as fallback if server ID is not specified.
     *
     * @param config Server configuration
     * @return Server ID
     */
    private String getServerId(MqttProperties.ServerConfig config) {
        if (!StringUtils.hasText(config.getId())) {
            return "default-" + config.getClientId();
        }
        return config.getId();
    }

    /**
     * Builds an MQTT5 async client with the given configuration.
     * Uses HiveMQ's built-in automatic reconnect mechanism.
     *
     * @param config Server configuration
     * @return Configured MQTT5 async client
     */
    private Mqtt5AsyncClient buildMqtt5AsyncClient(MqttProperties.ServerConfig config) {
        URI uri = parseUri(config.getServerUri());
        String serverId = getServerId(config);
        // @formatter:off
        Mqtt5ClientBuilder builder = Mqtt5Client.builder()
                .identifier(config.getClientId())
                .serverHost(uri.getHost())
                .serverPort(uri.getPort() > 0 ? uri.getPort() : getDefaultPort(uri.getScheme()))
                .automaticReconnect()
                .initialDelay(config.getInitialDelay(), TimeUnit.SECONDS)
                .maxDelay(config.getMaxDelay(), TimeUnit.SECONDS)
                .applyAutomaticReconnect()
                .addConnectedListener(context -> {
                    connectionStatus.put(serverId, true);
                    log.info("MQTT5 client [{}] connected successfully to {}:{}", config.getClientId(), uri.getHost(), uri.getPort());
                }).addDisconnectedListener(context -> {
                    connectionStatus.put(serverId, false);
                    //noinspection ConstantValue
                    if (context.getReconnector() != null) {
                        // HiveMQ is handling reconnection automatically
                        log.warn("MQTT5 client [{}] disconnected. HiveMQ will handle reconnection automatically. Reason: {}", config.getClientId(), context.getCause() != null ? context.getCause().getMessage() : "Unknown");
                    } else {
                        log.warn("MQTT5 client [{}] disconnected. Reason: {}", config.getClientId(), context.getCause() != null ? context.getCause().getMessage() : "Unknown");
                    }
                });

        configureAuthentication(builder, config);
        return builder.buildAsync();
    }

    /**
     * Configures authentication for the MQTT client if username and password are provided.
     *
     * @param builder MQTT client builder
     * @param config  Server configuration containing authentication credentials
     */
    private void configureAuthentication(Mqtt5ClientBuilder builder, MqttProperties.ServerConfig config) {
        if (StringUtils.hasText(config.getUsername()) && StringUtils.hasText(config.getPassword())) {
            Mqtt5SimpleAuth simpleAuth = Mqtt5SimpleAuth.builder().username(config.getUsername()).password(config.getPassword().getBytes(StandardCharsets.UTF_8)).build();
            builder.simpleAuth(simpleAuth);
        }
    }

    /**
     * Connects the MQTT client to the server with the specified configuration.
     *
     * @param client   MQTT client to connect
     * @param config   Server configuration
     * @param serverId Server identifier
     */
    private void connectClient(Mqtt5AsyncClient client, MqttProperties.ServerConfig config, String serverId) {
        // @formatter:off
        Mqtt5Connect connectMessage = Mqtt5Connect.builder()
                .cleanStart(config.isCleanStart())
                .sessionExpiryInterval(config.getSessionExpiryInterval())
                .keepAlive(config.getKeepAlive())
                .restrictions()
                .receiveMaximum(config.getReceiveMaximum())
                .maximumPacketSize(config.getMaximumPacketSize())
                .applyRestrictions()
                .build();
        // @formatter:on
        client.connect(connectMessage).whenComplete((connAck, throwable) -> {
            if (throwable != null) {
                if (throwable instanceof Mqtt5ConnAckException) {
                    Mqtt5ConnAckException connAckEx = (Mqtt5ConnAckException) throwable;
                    log.error("Failed to connect client [{}] to server {}: {}, reason code: {}", config.getClientId(), serverId, connAckEx.getMessage(), connAckEx.getMqttMessage().getReasonCode());
                } else {
                    log.error("Failed to connect client [{}] to server {}: {}", config.getClientId(), serverId, throwable.getMessage(), throwable);
                }
                connectionStatus.put(serverId, false);
            } else {
                connectionStatus.put(serverId, true);
                log.info("Successfully connected client [{}] to server {}. Session present: {}", config.getClientId(), serverId, connAck.isSessionPresent());
            }
        });
    }

    /**
     * Parses a URI string into a URI object.
     *
     * @param uriString URI string to parse
     * @return Parsed URI object
     * @throws IllegalArgumentException if URI is empty or invalid
     */
    private URI parseUri(String uriString) {
        if (!StringUtils.hasText(uriString)) {
            throw new IllegalArgumentException("MQTT server URI cannot be empty");
        }

        try {
            String normalizedUri = normalizeUri(uriString);
            return new URI(normalizedUri);
        } catch (URISyntaxException e) {
            log.error("Invalid MQTT URI format: {}", uriString, e);
            throw new IllegalArgumentException("Invalid MQTT URI format: " + uriString, e);
        }
    }

    /**
     * Normalizes a URI string by adding protocol if missing and trimming.
     *
     * @param uri URI string to normalize
     * @return Normalized URI string
     */
    private String normalizeUri(String uri) {
        uri = uri.trim();

        if (!uri.contains("://")) {
            if (uri.startsWith("//")) {
                uri = "tcp:" + uri;
            } else {
                uri = "tcp://" + uri;
            }
        }

        if (uri.endsWith(":")) {
            uri = uri.substring(0, uri.length() - 1);
        }

        return uri;
    }

    /**
     * Gets the default port for a given URI scheme.
     *
     * @param scheme URI scheme (tcp, ssl, ws, wss)
     * @return Default port number
     */
    private int getDefaultPort(String scheme) {
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
     * Gets the connection status for all configured MQTT clients.
     *
     * @return Map of server ID to connection status (true=connected, false=disconnected)
     */
    public Map<String, Boolean> getConnectionStatus() {
        return new ConcurrentHashMap<>(connectionStatus);
    }

    /**
     * Checks if a specific MQTT client is connected.
     *
     * @param serverId Server ID to check
     * @return true if connected, false otherwise
     */
    public boolean isConnected(String serverId) {
        return connectionStatus.getOrDefault(serverId, false);
    }
}
