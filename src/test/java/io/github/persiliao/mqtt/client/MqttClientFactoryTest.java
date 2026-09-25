package io.github.persiliao.mqtt.client;

import com.hivemq.client.mqtt.datatypes.MqttUtf8String;
import com.hivemq.client.mqtt.mqtt5.message.connect.Mqtt5Connect;
import io.github.persiliao.mqtt.autoconfigure.properties.MqttProperties;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for the URI handling of {@link MqttClientFactory}.
 *
 * @author Persi.Liao <xiangchu.liao@gmail.com>
 */
class MqttClientFactoryTest {

    @Test
    void addsTcpSchemeWhenMissing() {
        URI uri = MqttClientFactory.parseUri("localhost:1883");
        assertThat(uri.getScheme()).isEqualTo("tcp");
        assertThat(uri.getHost()).isEqualTo("localhost");
        assertThat(uri.getPort()).isEqualTo(1883);
    }

    @Test
    void addsTcpSchemeForDoubleSlashUri() {
        URI uri = MqttClientFactory.parseUri("//localhost:1883");
        assertThat(uri.getScheme()).isEqualTo("tcp");
        assertThat(uri.getHost()).isEqualTo("localhost");
    }

    @Test
    void appliesDefaultPortByScheme() {
        assertThat(MqttClientFactory.defaultPort(MqttClientFactory.parseUri("tcp://broker")))
                .isEqualTo(1883);
        assertThat(MqttClientFactory.defaultPort(MqttClientFactory.parseUri("ssl://broker")))
                .isEqualTo(8883);
    }

    @Test
    void explicitPortWins() {
        assertThat(MqttClientFactory.defaultPort(MqttClientFactory.parseUri("tcp://broker:1234")))
                .isEqualTo(1234);
        assertThat(MqttClientFactory.defaultPort(MqttClientFactory.parseUri("ssl://broker:443")))
                .isEqualTo(443);
    }

    @Test
    void detectsTlsScheme() {
        assertThat(MqttClientFactory.usesTls(MqttClientFactory.parseUri("ssl://broker:8883"))).isTrue();
        assertThat(MqttClientFactory.usesTls(MqttClientFactory.parseUri("tcp://broker:1883"))).isFalse();
    }

    @Test
    void invalidUriThrows() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> MqttClientFactory.parseUri("ht tp://x"))
                .withMessageContaining("Invalid MQTT server URI");
    }

    @Test
    void buildConnectUsesCleanStartByDefault() {
        // A fresh process cannot consume a resumed session (the client has no publish
        // handlers registered yet, so queued QoS 1 messages would be dropped); the
        // default must therefore start a clean session.
        assertThat(MqttClientFactory.buildConnect(serverConfig("tcp://localhost:1883", "client-a")).isCleanStart())
                .isTrue();
    }

    @Test
    void buildConnectRespectsExplicitCleanStart() {
        MqttProperties.ServerConfig server = serverConfig("tcp://localhost:1883", "client-a");
        server.setCleanStart(false);
        assertThat(MqttClientFactory.buildConnect(server).isCleanStart()).isFalse();
    }

    @Test
    void buildConnectCarriesSessionAndRestrictionSettings() {
        MqttProperties.ServerConfig server = serverConfig("tcp://localhost:1883", "client-a");
        server.setKeepAlive(30);
        server.setSessionExpiryInterval(120);
        server.setReceiveMaximum(64);
        server.setMaximumPacketSize(4096);
        Mqtt5Connect connect = MqttClientFactory.buildConnect(server);
        assertThat(connect.getKeepAlive()).isEqualTo(30);
        assertThat(connect.getSessionExpiryInterval()).isEqualTo(120);
        assertThat(connect.getRestrictions().getReceiveMaximum()).isEqualTo(64);
        assertThat(connect.getRestrictions().getMaximumPacketSize()).isEqualTo(4096);
    }

    @Test
    void buildConnectAddsSimpleAuthWhenConfigured() {
        MqttProperties.ServerConfig server = serverConfig("tcp://localhost:1883", "client-a");
        server.setUsername("user");
        server.setPassword("secret");
        Mqtt5Connect connect = MqttClientFactory.buildConnect(server);
        assertThat(connect.getSimpleAuth()).isPresent();
        assertThat(connect.getSimpleAuth().orElseThrow().getUsername())
                .hasValue(MqttUtf8String.of("user"));
    }

    private static MqttProperties.ServerConfig serverConfig(String serverUri, String clientId) {
        MqttProperties.ServerConfig server = new MqttProperties.ServerConfig();
        server.setServerUri(serverUri);
        server.setClientId(clientId);
        return server;
    }
}
