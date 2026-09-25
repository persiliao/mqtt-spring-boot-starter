package io.github.persiliao.mqtt.handler;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import io.github.persiliao.mqtt.MqttMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A handler bean annotated with {@link MqttMessageHandler} together with its
 * resolved handler methods and subscription metadata.
 *
 * @since 3.0.0
 * @author Persi.Liao <xiangchu.liao@gmail.com>
 */
public final class HandlerRegistration {

    private static final Logger log = LoggerFactory.getLogger(HandlerRegistration.class);

    private final Object bean;
    private final String beanName;
    private final MqttMessageHandler annotation;
    private final List<String> topics;
    private final List<HandlerMethod> methods;
    private final MqttQos qos;
    private final boolean allServers;
    private final Set<String> serverIds;
    private final AtomicReference<HandlerStatistics> statistics = new AtomicReference<>();

    private HandlerRegistration(Object bean, String beanName, MqttMessageHandler annotation,
                                List<String> topics, List<HandlerMethod> methods, MqttQos qos,
                                boolean allServers, Set<String> serverIds) {
        this.bean = bean;
        this.beanName = beanName;
        this.annotation = annotation;
        this.topics = topics;
        this.methods = methods;
        this.qos = qos;
        this.allServers = allServers;
        this.serverIds = serverIds;
    }

    /**
     * Builds a registration from a handler bean and its annotation.
     *
     * @param beanName   the bean name
     * @param bean       the bean instance (may be a proxy)
     * @param annotation the annotation found on the bean's user class
     * @return the registration
     */
    static HandlerRegistration create(String beanName, Object bean, MqttMessageHandler annotation) {
        // ultimateTargetClass() unwraps AOP proxies (JDK dynamic and CGLIB),
        // so handler methods declared on the target class are discovered even
        // when the bean is proxied.
        Class<?> beanClass = AopProxyUtils.ultimateTargetClass(bean);
        List<HandlerMethod> methods = new ArrayList<>();
        for (Method method : beanClass.getMethods()) {
            if (method.isBridge() || method.isSynthetic() || Modifier.isStatic(method.getModifiers())) {
                continue;
            }
            if (method.getParameterCount() < 1 || !method.getName().startsWith("handle")) {
                continue;
            }
            if (!HandlerMethod.isResolvable(method, annotation.autoDeserialize())) {
                log.debug("Skipping handler method {} of {}: POJO parameters require autoDeserialize=true",
                        method, beanName);
                continue;
            }
            methods.add(new HandlerMethod(method, annotation.autoDeserialize()));
        }

        Set<String> serverIds = new LinkedHashSet<>();
        if (annotation.serverIds().length > 0) {
            for (String id : annotation.serverIds()) {
                if (StringUtils.hasText(id)) {
                    serverIds.add(id.trim());
                }
            }
        } else if (StringUtils.hasText(annotation.serverId())) {
            serverIds.add(annotation.serverId().trim());
        }
        boolean allServers = serverIds.isEmpty() || serverIds.contains("*");
        serverIds.remove("*");

        int configuredQos = annotation.qos();
        MqttQos qos = MqttQos.fromCode(configuredQos);
        if (qos == null) {
            log.warn("Invalid qos={} on handler '{}'; falling back to QoS 0", configuredQos, beanName);
            qos = MqttQos.AT_MOST_ONCE;
        }

        List<String> topics = new ArrayList<>(annotation.topics().length);
        for (String topic : annotation.topics()) {
            if (StringUtils.hasText(topic)) {
                topics.add(topic.trim());
            }
        }

        return new HandlerRegistration(bean, beanName, annotation, List.copyOf(topics),
                methods, qos, allServers, serverIds);
    }

    /**
     * @return the handler bean instance to invoke methods on
     */
    Object getBean() {
        return bean;
    }

    /**
     * @return the bean name
     */
    String getBeanName() {
        return beanName;
    }

    /**
     * @return the original annotation (for retainAsPublished, dedup, ordering, ...)
     */
    MqttMessageHandler getAnnotation() {
        return annotation;
    }

    /**
     * @return the resolved QoS of the subscriptions
     */
    MqttQos getQos() {
        return qos;
    }

    /**
     * @return the topic filters to subscribe to
     */
    List<String> getTopics() {
        return topics;
    }

    /**
     * @return the resolved handler methods in declaration order
     */
    List<HandlerMethod> getMethods() {
        return methods;
    }

    /**
     * @param serverId the server id to test
     * @return {@code true} when this handler subscribes to that server
     */
    boolean targets(String serverId) {
        return allServers || serverIds.contains(serverId);
    }

    /**
     * Returns the statistics instance, creating it on first access when
     * statistics are enabled.
     *
     * @return the statistics, or {@code null} when disabled via the annotation
     */
    HandlerStatistics statistics() {
        if (!annotation.statistics()) {
            return null;
        }
        return statistics.updateAndGet(existing -> existing != null ? existing : new HandlerStatistics());
    }
}
