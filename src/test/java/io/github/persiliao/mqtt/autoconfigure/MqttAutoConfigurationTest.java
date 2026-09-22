package io.github.persiliao.mqtt.autoconfigure;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import io.github.persiliao.mqtt.MqttMessageHandler;
import io.github.persiliao.mqtt.autoconfigure.properties.MqttProperties;
import io.github.persiliao.mqtt.client.MqttClientFactory;
import io.github.persiliao.mqtt.client.MqttClientRegistry;
import io.github.persiliao.mqtt.handler.MqttMessageDispatcher;
import io.github.persiliao.mqtt.handler.MqttMessageHandlerProcessor;
import io.github.persiliao.mqtt.handler.MqttSubscriptionManager;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Auto-configuration tests using {@link ApplicationContextRunner}.
 *
 * <p>Note: the created clients try to connect to the configured (non-existent)
 * brokers asynchronously; those failures do not affect the application
 * context and are expected in these tests.
 */
class MqttAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MqttAutoConfiguration.class));

    private static final String[] SINGLE_MODE = {
            "mqtt.single-server.server-uri=tcp://localhost:1883",
            "mqtt.single-server.client-id=test-client"
    };

    @Test
    void singleModeCreatesClientAndMachinery() {
        contextRunner.withPropertyValues(SINGLE_MODE)
                .run(context -> {
                    assertThat(context).hasSingleBean(Mqtt5AsyncClient.class);
                    assertThat(context).hasBean("singleMqttClient");
                    assertThat(context).hasSingleBean(MqttClientRegistry.class);
                    assertThat(context).hasSingleBean(MqttClientFactory.class);
                    assertThat(context).hasSingleBean(MqttMessageDispatcher.class);
                    assertThat(context).hasSingleBean(MqttSubscriptionManager.class);
                    assertThat(context).hasSingleBean(MqttMessageHandlerProcessor.class);
                    assertThat(context).hasSingleBean(MqttProperties.class);
                });
    }

    @Test
    void disabledProducesNoBeans() {
        contextRunner.withPropertyValues("mqtt.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(Mqtt5AsyncClient.class);
                    assertThat(context).doesNotHaveBean(MqttClientRegistry.class);
                    assertThat(context).doesNotHaveBean(MqttSubscriptionManager.class);
                });
    }

    @Test
    void multiModeCreatesClientMap() {
        contextRunner.withPropertyValues(
                        "mqtt.mode=MULTI",
                        "mqtt.multi-server.servers[0].id=primary",
                        "mqtt.multi-server.servers[0].server-uri=tcp://localhost:1883",
                        "mqtt.multi-server.servers[0].client-id=client-a",
                        "mqtt.multi-server.servers[1].id=secondary",
                        "mqtt.multi-server.servers[1].server-uri=tcp://localhost:1884",
                        "mqtt.multi-server.servers[1].client-id=client-b")
                .run(context -> {
                    assertThat(context).hasBean("multiMqttClients");
                    assertThat(context).doesNotHaveBean("singleMqttClient");
                    Map<String, Mqtt5AsyncClient> clients =
                            context.getBean("multiMqttClients", Map.class);
                    assertThat(clients).containsKeys("primary", "secondary");
                });
    }

    @Test
    void missingClientIdFailsStartup() {
        contextRunner.withPropertyValues("mqtt.single-server.server-uri=tcp://localhost:1883")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void handlerBeanIsDiscoveredAndRegistered() {
        contextRunner.withPropertyValues(SINGLE_MODE)
                .withUserConfiguration(HandlerConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(TemperatureHandler.class);
                    MqttSubscriptionManager manager = context.getBean(MqttSubscriptionManager.class);
                    assertThat(manager.getTopics("temperatureHandler"))
                            .containsExactly("sensor/temperature");
                });
    }

    @Configuration
    static class HandlerConfig {

        @Bean
        public TemperatureHandler temperatureHandler() {
            return new TemperatureHandler();
        }
    }

    @MqttMessageHandler(topics = "sensor/temperature")
    static class TemperatureHandler {

        public void handleMessage(String topic, String payload) {
            // no-op
        }
    }
}
