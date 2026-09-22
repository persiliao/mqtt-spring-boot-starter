package io.github.persiliao.mqtt.handler;

import io.github.persiliao.mqtt.MqttMessageHandler;
import io.github.persiliao.mqtt.autoconfigure.properties.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Discovers beans annotated with {@link MqttMessageHandler} and registers them
 * with the {@link MqttSubscriptionManager}.
 *
 * <p>Discovery happens on {@link ContextRefreshedEvent} rather than inside a
 * {@code BeanPostProcessor}: at that moment every singleton — including the
 * MQTT clients created by the auto-configuration and the bound
 * {@link MqttProperties} — is already instantiated, which removes the
 * ordering race between handler beans and client beans.
 *
 * @since 3.0.0
 */
public class MqttMessageHandlerProcessor implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger log = LoggerFactory.getLogger(MqttMessageHandlerProcessor.class);

    private final MqttSubscriptionManager subscriptionManager;
    private final AtomicBoolean processed = new AtomicBoolean();

    /**
     * Creates the processor.
     *
     * @param subscriptionManager the manager handlers are registered with
     */
    public MqttMessageHandlerProcessor(MqttSubscriptionManager subscriptionManager) {
        this.subscriptionManager = subscriptionManager;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (!processed.compareAndSet(false, true)) {
            return;
        }

        ApplicationContext context = event.getApplicationContext();
        MqttProperties properties = context.getBean(MqttProperties.class);
        if (!properties.isEnabled()) {
            return;
        }

        Map<String, Object> beans = context.getBeansWithAnnotation(MqttMessageHandler.class);
        int registered = 0;
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            Object bean = entry.getValue();
            // ultimateTargetClass() also unwraps JDK dynamic proxies, whose
            // proxy class does not carry the annotation of the target class.
            Class<?> beanClass = AopProxyUtils.ultimateTargetClass(bean);
            MqttMessageHandler annotation = AnnotationUtils.findAnnotation(beanClass, MqttMessageHandler.class);
            if (annotation == null) {
                continue;
            }
            HandlerRegistration registration = HandlerRegistration.create(entry.getKey(), bean, annotation);
            if (registration.getTopics().isEmpty()) {
                log.warn("Bean '{}' is annotated with @MqttMessageHandler but declares no topic; "
                        + "it will not subscribe to anything", entry.getKey());
                continue;
            }
            if (registration.getMethods().isEmpty()) {
                log.warn("Bean '{}' is annotated with @MqttMessageHandler but declares no discoverable "
                                + "handler method (public non-static method starting with 'handle' "
                                + "with at least one resolvable parameter)", entry.getKey());
                continue;
            }
            subscriptionManager.registerHandler(registration);
            registered++;
        }
        if (registered > 0) {
            log.info("Registered {} MQTT message handler(s)", registered);
        }
    }
}
