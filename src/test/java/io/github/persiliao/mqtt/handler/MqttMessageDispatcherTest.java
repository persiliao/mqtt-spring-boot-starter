package io.github.persiliao.mqtt.handler;

import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.github.persiliao.mqtt.MqttMessageHandler;
import io.github.persiliao.mqtt.autoconfigure.properties.MqttProperties;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the resource bounding of {@link MqttMessageDispatcher}.
 *
 * <p>The dispatcher derives per-key state (ordered executors, deduplication
 * windows) from the topic of the <em>incoming</em> message. With a wildcard
 * subscription the number of distinct topics is unbounded, so both structures
 * must be bounded.
 *
 * @author Persi.Liao <xiangchu.liao@gmail.com>
 */
class MqttMessageDispatcherTest {

    @MqttMessageHandler(topics = "sensor/#", ordering = MqttMessageHandler.Ordering.PER_TOPIC)
    public static class OrderedHandler {
        public void handle(String topic, String payload) {
            // no-op: only the dispatch path is under test
        }
    }

    @MqttMessageHandler(topics = "sensor/#", deduplicate = true)
    public static class DeduplicatingHandler {
        public void handle(String topic, String payload) {
            // no-op
        }
    }

    /** Synchronous so the invocation count can be asserted without waiting. */
    @MqttMessageHandler(topics = "sensor/#", deduplicate = true, async = false)
    public static class SyncDeduplicatingHandler {
        final AtomicInteger invocations = new AtomicInteger();

        public void handle(String topic, String payload) {
            invocations.incrementAndGet();
        }
    }

    private static Mqtt5Publish publish(String topic, String payload) {
        return Mqtt5Publish.builder()
                .topic(topic)
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> internalMap(MqttMessageDispatcher dispatcher, String fieldName) throws Exception {
        Field field = MqttMessageDispatcher.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return (Map<String, ?>) field.get(dispatcher);
    }

    private static HandlerRegistration registration(Object bean) {
        MqttMessageHandler annotation = bean.getClass().getAnnotation(MqttMessageHandler.class);
        return HandlerRegistration.create(bean.getClass().getSimpleName(), bean, annotation);
    }

    @Test
    void orderedExecutorsAreBounded() throws Exception {
        MqttMessageDispatcher dispatcher =
                new MqttMessageDispatcher(new MqttProperties(), Mockito.mock(ApplicationContext.class));
        HandlerRegistration registration = registration(new OrderedHandler());
        try {
            for (int i = 0; i < 500; i++) {
                dispatcher.dispatch(publish("sensor/" + i, "42"), registration, "default");
            }
            // 500 distinct topics must not create 500 single-thread executors.
            assertThat(internalMap(dispatcher, "orderedExecutors")).hasSizeLessThanOrEqualTo(64);
        } finally {
            dispatcher.destroy();
        }
    }

    @Test
    void deduplicationCachesAreBounded() throws Exception {
        MqttMessageDispatcher dispatcher =
                new MqttMessageDispatcher(new MqttProperties(), Mockito.mock(ApplicationContext.class));
        HandlerRegistration registration = registration(new DeduplicatingHandler());
        try {
            for (int i = 0; i < 3000; i++) {
                dispatcher.dispatch(publish("sensor/" + i, "42"), registration, "default");
            }
            assertThat(internalMap(dispatcher, "deduplicationCaches")).hasSizeLessThanOrEqualTo(1024);
        } finally {
            dispatcher.destroy();
        }
    }

    @Test
    void duplicateMessagesAreSuppressed() {
        MqttMessageDispatcher dispatcher =
                new MqttMessageDispatcher(new MqttProperties(), Mockito.mock(ApplicationContext.class));
        SyncDeduplicatingHandler bean = new SyncDeduplicatingHandler();
        HandlerRegistration registration = registration(bean);
        try {
            dispatcher.dispatch(publish("sensor/1", "same"), registration, "default");
            dispatcher.dispatch(publish("sensor/1", "same"), registration, "default");
            dispatcher.dispatch(publish("sensor/1", "different"), registration, "default");

            assertThat(bean.invocations.get()).isEqualTo(2);
        } finally {
            dispatcher.destroy();
        }
    }
}
