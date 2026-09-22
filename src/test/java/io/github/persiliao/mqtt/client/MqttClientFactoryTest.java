package io.github.persiliao.mqtt.client;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Unit tests for the URI handling of {@link MqttClientFactory}.
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
}
