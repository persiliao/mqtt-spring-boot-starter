package io.github.persiliao.mqtt.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.persiliao.mqtt.MqttMessageContext;
import io.github.persiliao.mqtt.MqttMessageConversionException;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for the parameter resolution and invocation of {@link HandlerMethod}.
 */
class HandlerMethodTest {

    public static class Reading {
        public double temperature;
        public String device;
    }

    // Public because the starter invokes handler methods reflectively.
    public static class CapturingHandler {

        // plain HashMap: unlike ConcurrentHashMap it accepts null values
        final Map<String, Object> captured = new HashMap<>();

        public void handleMessage(String topic, String payload) {
            captured.put("topic", topic);
            captured.put("payload", payload);
        }

        public void handleRaw(String topic, byte[] raw) {
            captured.put("raw", raw);
        }

        public void handlePublish(Mqtt5Publish publish) {
            captured.put("publish", publish);
        }

        public void handleContext(String topic, MqttMessageContext context) {
            captured.put("serverId", context.serverId());
            captured.put("contextTopic", context.topic());
        }

        public void handleJson(String topic, Map<String, Object> json) {
            captured.put("json", json);
        }

        public void handlePojo(String topic, Reading reading) {
            captured.put("pojo", reading);
        }
    }

    private static Mqtt5Publish publish(String topic, String payload) {
        return Mqtt5Publish.builder()
                .topic(topic)
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    @Test
    void resolvesTopicAndTextPayload() throws Exception {
        CapturingHandler handler = new CapturingHandler();
        Method method = CapturingHandler.class.getMethod("handleMessage", String.class, String.class);
        new HandlerMethod(method, true)
                .invoke(handler, publish("a/b", "42"), "server-1", new ObjectMapper());

        assertThat(handler.captured).containsEntry("topic", "a/b").containsEntry("payload", "42");
    }

    @Test
    void resolvesRawPayload() throws Exception {
        CapturingHandler handler = new CapturingHandler();
        Method method = CapturingHandler.class.getMethod("handleRaw", String.class, byte[].class);
        new HandlerMethod(method, true)
                .invoke(handler, publish("a/b", "raw"), "server-1", new ObjectMapper());

        assertThat((byte[]) handler.captured.get("raw")).containsExactly('r', 'a', 'w');
    }

    @Test
    void resolvesPublishMessage() throws Exception {
        CapturingHandler handler = new CapturingHandler();
        Method method = CapturingHandler.class.getMethod("handlePublish", Mqtt5Publish.class);
        Mqtt5Publish publish = publish("a/b", "x");
        new HandlerMethod(method, true)
                .invoke(handler, publish, "server-1", new ObjectMapper());

        assertThat(handler.captured.get("publish")).isSameAs(publish);
    }

    @Test
    void resolvesContext() throws Exception {
        CapturingHandler handler = new CapturingHandler();
        Method method = CapturingHandler.class.getMethod("handleContext", String.class, MqttMessageContext.class);
        new HandlerMethod(method, true)
                .invoke(handler, publish("a/b", "x"), "server-2", new ObjectMapper());

        assertThat(handler.captured).containsEntry("serverId", "server-2").containsEntry("contextTopic", "a/b");
    }

    @Test
    void resolvesJsonToMap() throws Exception {
        CapturingHandler handler = new CapturingHandler();
        Method method = CapturingHandler.class.getMethod("handleJson", String.class, Map.class);
        new HandlerMethod(method, true)
                .invoke(handler, publish("a/b", "{\"value\":7}"), "server-1", new ObjectMapper());

        @SuppressWarnings("unchecked")
        Map<String, Object> json = (Map<String, Object>) handler.captured.get("json");
        assertThat(json).containsEntry("value", 7);
    }

    @Test
    void resolvesJsonToPojo() throws Exception {
        CapturingHandler handler = new CapturingHandler();
        Method method = CapturingHandler.class.getMethod("handlePojo", String.class, Reading.class);
        new HandlerMethod(method, true)
                .invoke(handler,
                        publish("a/b", "{\"temperature\":21.5,\"device\":\"room\"}"),
                        "server-1", new ObjectMapper());

        Reading reading = (Reading) handler.captured.get("pojo");
        assertThat(reading.temperature).isEqualTo(21.5);
        assertThat(reading.device).isEqualTo("room");
    }

    @Test
    void malformedJsonThrowsConversionException() throws Exception {
        Method method = CapturingHandler.class.getMethod("handlePojo", String.class, Reading.class);
        HandlerMethod handlerMethod = new HandlerMethod(method, true);

        assertThatThrownBy(() -> handlerMethod
                .invoke(new CapturingHandler(), publish("a/b", "not-json"), "server-1", new ObjectMapper()))
                .isInstanceOf(MqttMessageConversionException.class)
                .hasMessageContaining("a/b");
    }

    @Test
    void missingObjectMapperThrowsConversionExceptionForPojo() throws Exception {
        Method method = CapturingHandler.class.getMethod("handlePojo", String.class, Reading.class);
        HandlerMethod handlerMethod = new HandlerMethod(method, true);

        assertThatThrownBy(() -> handlerMethod
                .invoke(new CapturingHandler(), publish("a/b", "{\"a\":1}"), "server-1", null))
                .isInstanceOf(MqttMessageConversionException.class);
    }

    @Test
    void emptyPayloadYieldsNullPojo() throws Exception {
        CapturingHandler handler = new CapturingHandler();
        Method method = CapturingHandler.class.getMethod("handlePojo", String.class, Reading.class);
        new HandlerMethod(method, true)
                .invoke(handler, publish("a/b", ""), "server-1", new ObjectMapper());

        assertThat(handler.captured.get("pojo")).isNull();
    }

    @Test
    void pojoParameterRequiresAutoDeserialize() throws Exception {
        Method pojo = CapturingHandler.class.getMethod("handlePojo", String.class, Reading.class);
        assertThat(HandlerMethod.isResolvable(pojo, false)).isFalse();
        assertThat(HandlerMethod.isResolvable(pojo, true)).isTrue();

        Method text = CapturingHandler.class.getMethod("handleMessage", String.class, String.class);
        assertThat(HandlerMethod.isResolvable(text, false)).isTrue();
    }
}
