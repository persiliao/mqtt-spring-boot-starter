package io.github.persiliao.mqtt.client;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5ConnectBuilder;
import io.github.persiliao.mqtt.autoconfigure.properties.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * Builds, connects and registers {@link Mqtt5AsyncClient} instances from
 * {@link MqttProperties.ServerConfig} entries.
 *
 * <p>The connection is asynchronous: the returned client is already registered
 * in the {@link MqttClientRegistry} and connecting in the background.
 * Subscriptions are therefore deferred by the
 * {@link MqttSubscriptionManager} until the client reports a connected state.
 *
 * @since 3.0.0
 */
public class MqttClientFactory {

    private static final Logger log = LoggerFactory.getLogger(MqttClientFactory.class);

    private final MqttClientRegistry registry;

    /**
     * Creates a factory.
     *
     * @param registry the registry clients are registered into
     */
    public MqttClientFactory(MqttClientRegistry registry) {
        this.registry = registry;
    }

    /**
     * Creates a client for the given configuration, registers it and starts
     * the asynchronous connection.
     *
     * @param config the server configuration
     * @return the created client (connecting or already connected)
     */
    public Mqtt5AsyncClient create(MqttProperties.ServerConfig config) {
        String serverId = config.resolveId();
        Mqtt5AsyncClient client = build(config, serverId);
        registry.register(serverId, client);
        connect(client, config, serverId);
        return client;
    }

    private Mqtt5AsyncClient build(MqttProperties.ServerConfig config, String serverId) {
        URI uri = parseUri(config.getServerUri());

        Mqtt5ClientBuilder builder = Mqtt5Client.builder()
                .identifier(config.getClientId())
                .serverHost(uri.getHost())
                .serverPort(defaultPort(uri));

        if (usesTls(uri)) {
            builder.sslWithDefaultConfig();
        }
        if (config.isAutomaticReconnect()) {
            builder.automaticReconnect()
                    .initialDelay(config.getInitialDelay().toMillis(), TimeUnit.MILLISECONDS)
                    .maxDelay(config.getMaxDelay().toMillis(), TimeUnit.MILLISECONDS)
                    .applyAutomaticReconnect();
        }
        builder.addConnectedListener(context -> {
            log.info("MQTT client [{}] connected to {}", config.getClientId(), config.getServerUri());
            registry.markConnected(serverId);
        });
        builder.addDisconnectedListener(context -> {
            String reason = context.getCause() != null ? context.getCause().getMessage() : "unknown";
            log.warn("MQTT client [{}] disconnected ({}); automatic reconnect {}",
                    config.getClientId(), reason,
                    context.getReconnector() != null ? "is active" : "is disabled");
            registry.markDisconnected(serverId);
        });

        return builder.buildAsync();
    }

    private void connect(Mqtt5AsyncClient client, MqttProperties.ServerConfig config, String serverId) {
        Mqtt5ConnectBuilder connectBuilder = Mqtt5Connect.builder()
                .keepAlive(config.getKeepAlive())
                .sessionExpiryInterval(config.getSessionExpiryInterval())
                .cleanStart(config.isCleanStart())
                .restrictions()
                .receiveMaximum(config.getReceiveMaximum())
                .maximumPacketSize(config.getMaximumPacketSize())
                .applyRestrictions();

        if (config.hasAuthentication()) {
            connectBuilder.simpleAuth(Mqtt5SimpleAuth.builder()
                    .username(config.getUsername())
                    .password(config.getPassword().getBytes(StandardCharsets.UTF_8))
                    .build());
        }

        client.connect(connectBuilder.build()).whenComplete((connAck, error) -> {
            if (error != null) {
                log.error("MQTT client [{}] failed to connect to {} (serverId={}): {}",
                        config.getClientId(), config.getServerUri(), serverId, error.toString());
            } else {
                log.info("MQTT client [{}] connected to {} (serverId={}, sessionPresent={})",
                        config.getClientId(), config.getServerUri(), serverId, connAck.isSessionPresent());
            }
        });
    }

    /**
     * Parses a server URI; a missing scheme defaults to {@code tcp}.
     *
     * @param raw the raw uri (e.g. {@code tcp://host:1883} or {@code host:1883})
     * @return the parsed uri
     * @throws IllegalArgumentException if the uri cannot be parsed
     */
    static URI parseUri(String raw) {
        String uri = raw == null ? "" : raw.trim();
        if (!uri.contains("://") && !uri.startsWith("//")) {
            uri = "tcp://" + uri;
        } else if (uri.startsWith("//")) {
            uri = "tcp:" + uri;
        }
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid MQTT server URI: " + raw, e);
        }
    }

    /**
     * Returns the effective port of the uri, applying the scheme default when
     * no explicit port is present ({@code tcp} → 1883, {@code ssl} → 8883).
     *
     * @param uri the parsed uri
     * @return the port
     */
    static int defaultPort(URI uri) {
        if (uri.getPort() > 0) {
            return uri.getPort();
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase();
        return switch (scheme) {
            case "ssl" -> 8883;
            default -> 1883;
        };
    }

    /**
     * @param uri the parsed uri
     * @return {@code true} when the scheme requires TLS
     */
    static boolean usesTls(URI uri) {
        return "ssl".equalsIgnoreCase(uri.getScheme());
    }
}
