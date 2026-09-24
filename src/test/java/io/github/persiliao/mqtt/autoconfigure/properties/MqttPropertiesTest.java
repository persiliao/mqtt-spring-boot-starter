package io.github.persiliao.mqtt.autoconfigure.properties;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Unit tests for {@link MqttProperties#validate()}.
 */
class MqttPropertiesTest {

    private MqttProperties singleMode(String serverUri, String clientId) {
        MqttProperties properties = new MqttProperties();
        MqttProperties.ServerConfig server = new MqttProperties.ServerConfig();
        server.setServerUri(serverUri);
        server.setClientId(clientId);
        properties.setSingleServer(server);
        return properties;
    }

    @Test
    void cleanStartDefaultsToTrue() {
        assertThat(new MqttProperties.ServerConfig().isCleanStart()).isTrue();
    }

    @Test
    void validSingleModePasses() {
        assertThatCode(() -> singleMode("tcp://localhost:1883", "client-a").validate())
                .doesNotThrowAnyException();
    }

    @Test
    void singleModeWithoutServerConfigFails() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new MqttProperties().validate())
                .withMessageContaining("mqtt.single-server");
    }

    @Test
    void singleModeMissingClientIdFails() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> singleMode("tcp://localhost:1883", " ").validate())
                .withMessageContaining("client-id");
    }

    @Test
    void multiModePassesWithUniqueIds() {
        MqttProperties properties = new MqttProperties();
        properties.setMode(MqttProperties.Mode.MULTI);
        MqttProperties.MultiServerConfig multi = new MqttProperties.MultiServerConfig();
        MqttProperties.ServerConfig first = new MqttProperties.ServerConfig();
        first.setId("primary");
        first.setServerUri("tcp://localhost:1883");
        first.setClientId("client-a");
        MqttProperties.ServerConfig second = new MqttProperties.ServerConfig();
        second.setId("secondary");
        second.setServerUri("tcp://localhost:1884");
        second.setClientId("client-b");
        multi.setServers(java.util.List.of(first, second));
        properties.setMultiServer(multi);

        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    @Test
    void multiModeWithoutServersFails() {
        MqttProperties properties = new MqttProperties();
        properties.setMode(MqttProperties.Mode.MULTI);
        MqttProperties.MultiServerConfig multi = new MqttProperties.MultiServerConfig();
        multi.setServers(java.util.List.of());
        properties.setMultiServer(multi);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("mqtt.multi-server.servers");
    }

    @Test
    void multiModeMissingServerIdFails() {
        MqttProperties properties = new MqttProperties();
        properties.setMode(MqttProperties.Mode.MULTI);
        MqttProperties.MultiServerConfig multi = new MqttProperties.MultiServerConfig();
        MqttProperties.ServerConfig server = new MqttProperties.ServerConfig();
        server.setServerUri("tcp://localhost:1883");
        server.setClientId("client-a");
        multi.setServers(java.util.List.of(server));
        properties.setMultiServer(multi);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining(".id is required");
    }

    @Test
    void multiModeDuplicateServerIdsFail() {
        MqttProperties properties = new MqttProperties();
        properties.setMode(MqttProperties.Mode.MULTI);
        MqttProperties.MultiServerConfig multi = new MqttProperties.MultiServerConfig();
        MqttProperties.ServerConfig first = new MqttProperties.ServerConfig();
        first.setId("same");
        first.setServerUri("tcp://localhost:1883");
        first.setClientId("client-a");
        MqttProperties.ServerConfig second = new MqttProperties.ServerConfig();
        second.setId("same");
        second.setServerUri("tcp://localhost:1884");
        second.setClientId("client-b");
        multi.setServers(java.util.List.of(first, second));
        properties.setMultiServer(multi);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("Duplicate server id");
    }

    @Test
    void disabledSkipsValidation() {
        MqttProperties properties = new MqttProperties();
        properties.setEnabled(false);

        assertThatCode(properties::validate).doesNotThrowAnyException();
    }

    @Test
    void keepAliveOutOfRangeFails() {
        MqttProperties properties = singleMode("tcp://localhost:1883", "client-a");
        properties.getSingleServer().setKeepAlive(70000);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("keep-alive");
    }

    @Test
    void receiveMaximumMustBeWithinMqttRange() {
        MqttProperties properties = singleMode("tcp://localhost:1883", "client-a");
        properties.getSingleServer().setReceiveMaximum(0);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("receive-maximum");
    }

    @Test
    void maximumPacketSizeMustBePositive() {
        MqttProperties properties = singleMode("tcp://localhost:1883", "client-a");
        properties.getSingleServer().setMaximumPacketSize(0);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("maximum-packet-size");
    }

    @Test
    void sessionExpiryIntervalOutOfRangeFails() {
        MqttProperties properties = singleMode("tcp://localhost:1883", "client-a");
        properties.getSingleServer().setSessionExpiryInterval(-1L);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("session-expiry-interval");
    }

    @Test
    void nullReconnectDelayFails() {
        MqttProperties properties = singleMode("tcp://localhost:1883", "client-a");
        properties.getSingleServer().setInitialDelay(null);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("initial-delay");
    }

    @Test
    void initialDelayMustNotExceedMaxDelay() {
        MqttProperties properties = singleMode("tcp://localhost:1883", "client-a");
        properties.getSingleServer().setInitialDelay(java.time.Duration.ofSeconds(30));
        properties.getSingleServer().setMaxDelay(java.time.Duration.ofSeconds(1));

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("must not exceed max-delay");
    }

    @Test
    void duplicateServerIdsDifferingOnlyByWhitespaceFail() {
        MqttProperties properties = new MqttProperties();
        properties.setMode(MqttProperties.Mode.MULTI);
        MqttProperties.MultiServerConfig multi = new MqttProperties.MultiServerConfig();
        MqttProperties.ServerConfig first = new MqttProperties.ServerConfig();
        first.setId("primary");
        first.setServerUri("tcp://localhost:1883");
        first.setClientId("client-a");
        MqttProperties.ServerConfig second = new MqttProperties.ServerConfig();
        second.setId(" primary ");
        second.setServerUri("tcp://localhost:1884");
        second.setClientId("client-b");
        multi.setServers(java.util.List.of(first, second));
        properties.setMultiServer(multi);

        assertThatIllegalArgumentException()
                .isThrownBy(properties::validate)
                .withMessageContaining("Duplicate server id");
    }

    @Test
    void serverIdDefaultsToDefault() {
        MqttProperties.ServerConfig server = new MqttProperties.ServerConfig();
        assertThat(server.resolveId()).isEqualTo("default");
        server.setId("primary");
        assertThat(server.resolveId()).isEqualTo("primary");
    }

    @Test
    void toStringDoesNotLeakPassword() {
        MqttProperties.ServerConfig server = new MqttProperties.ServerConfig();
        server.setServerUri("tcp://localhost:1883");
        server.setClientId("client-a");
        server.setPassword("secret");

        assertThat(server.toString()).doesNotContain("secret");
    }
}
