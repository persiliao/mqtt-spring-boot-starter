package io.github.persiliao.mqtt.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.github.persiliao.mqtt.MqttMessageContext;
import io.github.persiliao.mqtt.MqttMessageConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A resolved handler method together with one parameter resolver per declared
 * parameter.
 *
 * <p>Parameter resolution rules (by declared type):
 * <ul>
 *   <li>{@link Mqtt5Publish} — the publish message itself</li>
 *   <li>{@link MqttMessageContext} — the envelope (topic, payload, server id, message)</li>
 *   <li>{@code byte[]} — the raw payload</li>
 *   <li>{@code String} — the topic for the first parameter, the payload
 *       decoded as UTF-8 otherwise</li>
 *   <li>{@code Map} / POJO — the payload deserialized from JSON</li>
 * </ul>
 *
 * @since 3.0.0
 */
final class HandlerMethod {

    private static final Logger log = LoggerFactory.getLogger(HandlerMethod.class);

    /**
     * Resolves the value of a single handler method parameter.
     */
    @FunctionalInterface
    interface ParameterResolver {
        Object resolve(Mqtt5Publish publish, String serverId, ObjectMapper objectMapper);
    }

    private final Method method;
    private final List<ParameterResolver> resolvers;
    private final boolean autoDeserialize;

    /**
     * Resolves the parameter resolvers of the method.
     *
     * @param method          the handler method
     * @param autoDeserialize whether JSON deserialization is enabled
     */
    HandlerMethod(Method method, boolean autoDeserialize) {
        this.method = method;
        this.autoDeserialize = autoDeserialize;
        this.resolvers = new ArrayList<>(method.getParameterCount());
        Parameter[] parameters = method.getParameters();
        for (int i = 0; i < parameters.length; i++) {
            resolvers.add(resolverFor(parameters[i].getType(), i));
        }
    }

    /**
     * @return the underlying reflection method
     */
    Method getMethod() {
        return method;
    }

    /**
     * Checks whether every parameter of the method can be resolved.
     *
     * @param method          the candidate method
     * @param autoDeserialize whether JSON deserialization is enabled
     * @return {@code true} when the method is invokable by the starter
     */
    static boolean isResolvable(Method method, boolean autoDeserialize) {
        for (Class<?> type : method.getParameterTypes()) {
            if (type == String.class || type == byte[].class || type == Mqtt5Publish.class
                    || type == MqttMessageContext.class || Map.class.isAssignableFrom(type)) {
                continue;
            }
            // POJO parameter: only resolvable when JSON deserialization is enabled
            if (!autoDeserialize) {
                return false;
            }
        }
        return true;
    }

    /**
     * Invokes the method on the given bean with the resolved arguments.
     *
     * @param bean         the handler bean instance (may be a proxy)
     * @param publish      the incoming publish
     * @param serverId     the server id the message arrived on
     * @param objectMapper the mapper used for JSON deserialization (may be null)
     * @return the return value of the invocation
     */
    Object invoke(Object bean, Mqtt5Publish publish, String serverId, ObjectMapper objectMapper) {
        Object[] args = new Object[resolvers.size()];
        for (int i = 0; i < args.length; i++) {
            args[i] = resolvers.get(i).resolve(publish, serverId, objectMapper);
        }
        return ReflectionUtils.invokeMethod(method, bean, args);
    }

    private ParameterResolver resolverFor(Class<?> type, int index) {
        if (type == Mqtt5Publish.class) {
            return (publish, serverId, objectMapper) -> publish;
        }
        if (type == MqttMessageContext.class) {
            return (publish, serverId, objectMapper) -> new MqttMessageContext(
                    publish.getTopic().toString(), publish.getPayloadAsBytes(), serverId, publish);
        }
        if (type == byte[].class) {
            return (publish, serverId, objectMapper) -> publish.getPayloadAsBytes();
        }
        if (type == String.class) {
            return index == 0
                    ? (publish, serverId, objectMapper) -> publish.getTopic().toString()
                    : (publish, serverId, objectMapper)
                            -> new String(publish.getPayloadAsBytes(), StandardCharsets.UTF_8);
        }
        if (Map.class.isAssignableFrom(type)) {
            return (publish, serverId, objectMapper) -> toMap(publish, objectMapper);
        }
        return (publish, serverId, objectMapper) -> toPojo(publish, type, objectMapper);
    }

    private static Map<String, Object> toMap(Mqtt5Publish publish, ObjectMapper objectMapper) {
        byte[] payload = publish.getPayloadAsBytes();
        if (payload.length == 0) {
            return Map.of();
        }
        if (objectMapper == null) {
            log.warn("ObjectMapper is not available; JSON payload on topic '{}' cannot be deserialized",
                    publish.getTopic());
            throw new MqttMessageConversionException(
                    "ObjectMapper is not available to deserialize the JSON payload on topic '"
                            + publish.getTopic() + "'", null);
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> result = objectMapper.readValue(payload, Map.class);
            return result;
        } catch (Exception e) {
            throw new MqttMessageConversionException("Failed to deserialize JSON payload on topic '"
                    + publish.getTopic() + "' to Map: " + e.getMessage(), e);
        }
    }

    private Object toPojo(Mqtt5Publish publish, Class<?> type, ObjectMapper objectMapper) {
        byte[] payload = publish.getPayloadAsBytes();
        if (payload.length == 0) {
            return null;
        }
        if (!autoDeserialize) {
            // Deserialization disabled: hand over the raw text and let the
            // invocation fail loudly instead of guessing.
            return new String(payload, StandardCharsets.UTF_8);
        }
        if (objectMapper == null) {
            throw new MqttMessageConversionException(
                    "ObjectMapper is not available to deserialize the JSON payload on topic '"
                            + publish.getTopic() + "' to " + type.getSimpleName(), null);
        }
        try {
            return objectMapper.readValue(payload, type);
        } catch (Exception e) {
            throw new MqttMessageConversionException("Failed to deserialize JSON payload on topic '"
                    + publish.getTopic() + "' to " + type.getSimpleName() + ": " + e.getMessage(), e);
        }
    }
}
