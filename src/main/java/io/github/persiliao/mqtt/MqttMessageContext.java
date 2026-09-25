package io.github.persiliao.mqtt;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import java.nio.charset.StandardCharsets;

/**
 * Envelope passed to handler methods that need more than the topic and the
 * payload.
 *
 * <p>Declare it as a parameter to access the originating server id and the
 * raw publish message:
 * <pre>{@code
 * public void handle(String topic, MqttMessageContext context) {
 *     String fromServer = context.serverId();
 *     byte[] raw = context.payload();
 *     String text = context.payloadAsText();
 * }
 * }</pre>
 *
 * <p>Note that the {@code payload} is a {@code byte[]}; as with every record
 * containing an array, {@code equals}/{@code hashCode} use identity for that
 * component.
 *
 * @param topic    the topic the message was published to
 * @param payload  the raw payload bytes
 * @param serverId the id of the server connection the message arrived on
 * @param message  the full MQTT 5 publish message
 * @since 3.0.0
 * @author Persi.Liao <xiangchu.liao@gmail.com>
 */
public record MqttMessageContext(String topic, byte[] payload, String serverId, Mqtt5Publish message) {

    /**
     * Decodes the payload as a UTF-8 string.
     *
     * @return the payload as text
     */
    public String payloadAsText() {
        return new String(payload, StandardCharsets.UTF_8);
    }
}
