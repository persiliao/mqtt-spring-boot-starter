package io.github.persiliao.mqtt.autoconfigure;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import io.github.persiliao.mqtt.autoconfigure.properties.MqttProperties;
import io.github.persiliao.mqtt.client.MqttClientFactory;
import io.github.persiliao.mqtt.client.MqttClientRegistry;
import io.github.persiliao.mqtt.handler.MqttMessageDispatcher;
import io.github.persiliao.mqtt.handler.MqttMessageHandlerProcessor;
import io.github.persiliao.mqtt.handler.MqttSubscriptionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Auto-configuration for the MQTT starter.
 *
 * <p>Creates the MQTT client(s) according to {@link MqttProperties} and wires
 * the message handler machinery (registry, dispatcher, subscription manager,
 * handler processor). Activated when the HiveMQ client is on the classpath and
 * {@code mqtt.enabled} is not explicitly set to {@code false}.
 *
 * @since 3.0.0
 * @author Persi.Liao <xiangchu.liao@gmail.com>
 */
@AutoConfiguration
@ConditionalOnClass(Mqtt5Client.class)
@ConditionalOnProperty(prefix = "mqtt", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(MqttProperties.class)
public class MqttAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MqttAutoConfiguration.class);

    /**
     * Registry of the created clients and their connection state.
     *
     * @return the registry
     */
    @Bean
    @ConditionalOnMissingBean
    public MqttClientRegistry mqttClientRegistry() {
        return new MqttClientRegistry();
    }

    /**
     * Per-message pipeline (deduplication, validation, dispatch, statistics).
     *
     * @param properties       the MQTT properties
     * @param applicationContext the context used to lazily resolve the ObjectMapper
     * @return the dispatcher
     */
    @Bean
    @ConditionalOnMissingBean
    public MqttMessageDispatcher mqttMessageDispatcher(MqttProperties properties,
                                                       ApplicationContext applicationContext) {
        return new MqttMessageDispatcher(properties, applicationContext);
    }

    /**
     * Subscription lifecycle manager.
     *
     * @param registry   the client registry
     * @param dispatcher the message dispatcher
     * @return the subscription manager
     */
    @Bean
    @ConditionalOnMissingBean
    public MqttSubscriptionManager mqttSubscriptionManager(MqttClientRegistry registry,
                                                           MqttMessageDispatcher dispatcher) {
        MqttSubscriptionManager subscriptionManager = new MqttSubscriptionManager(registry, dispatcher);
        // Explicit wiring: the manager only reacts to connect/disconnect
        // events once it is attached to the registry.
        subscriptionManager.attach();
        return subscriptionManager;
    }

    /**
     * Client factory.
     *
     * @param registry the client registry
     * @return the factory
     */
    @Bean
    @ConditionalOnMissingBean
    public MqttClientFactory mqttClientFactory(MqttClientRegistry registry) {
        return new MqttClientFactory(registry);
    }

    /**
     * The single MQTT client, created in {@code SINGLE} mode (the default).
     *
     * @param factory    the client factory
     * @param properties the MQTT properties
     * @return the client bean (named {@code singleMqttClient})
     */
    @Bean
    @ConditionalOnMissingBean(name = "singleMqttClient")
    @ConditionalOnProperty(prefix = "mqtt", name = "mode", havingValue = "SINGLE", matchIfMissing = true)
    public Mqtt5AsyncClient singleMqttClient(MqttClientFactory factory, MqttProperties properties) {
        properties.validate();
        return factory.create(properties.getSingleServer());
    }

    /**
     * One MQTT client per configured server, created in {@code MULTI} mode.
     *
     * @param factory    the client factory
     * @param properties the MQTT properties
     * @return the client map bean (named {@code multiMqttClients}), keyed by server id
     */
    @Bean
    @ConditionalOnMissingBean(name = "multiMqttClients")
    @ConditionalOnProperty(prefix = "mqtt", name = "mode", havingValue = "MULTI")
    public Map<String, Mqtt5AsyncClient> multiMqttClients(MqttClientFactory factory, MqttProperties properties) {
        properties.validate();
        Map<String, Mqtt5AsyncClient> clients = new LinkedHashMap<>();
        for (MqttProperties.ServerConfig config : properties.getMultiServer().getServers()) {
            try {
                clients.put(config.resolveId(), factory.create(config));
            } catch (Exception e) {
                log.error("Failed to create MQTT client for server '{}'", config.resolveId(), e);
                if (properties.getMultiServer().isFailFast()) {
                    throw new IllegalStateException(
                            "Failed to create MQTT client for server: " + config.resolveId(), e);
                }
            }
        }
        return clients;
    }

    /**
     * Discovers and registers {@code @MqttMessageHandler} beans.
     *
     * @param subscriptionManager the subscription manager
     * @return the processor
     */
    @Bean
    @ConditionalOnMissingBean
    public MqttMessageHandlerProcessor mqttMessageHandlerProcessor(MqttSubscriptionManager subscriptionManager) {
        return new MqttMessageHandlerProcessor(subscriptionManager);
    }
}
