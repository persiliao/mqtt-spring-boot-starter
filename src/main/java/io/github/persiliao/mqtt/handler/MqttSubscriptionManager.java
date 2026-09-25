package io.github.persiliao.mqtt.handler;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import io.github.persiliao.mqtt.MqttMessageHandler;
import io.github.persiliao.mqtt.client.MqttClientRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages the subscription lifecycle of the registered
 * {@link MqttMessageHandler} beans.
 *
 * <p>Subscriptions are (re)established every time a client reports a
 * connected state, which covers both the initial connection and every
 * automatic reconnect. On disconnect the "already subscribed" marks are
 * dropped so the next connection re-subscribes even if the client does not
 * auto-resubscribe on its own. Transient subscription failures are retried
 * on a shared scheduler.
 *
 * @since 3.0.0
 * @author Persi.Liao <xiangchu.liao@gmail.com>
 */
public class MqttSubscriptionManager implements MqttClientRegistry.ClientEventListener, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MqttSubscriptionManager.class);

    private static final int MAX_SUBSCRIPTION_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 2000L;

    private final MqttClientRegistry registry;
    private final MqttMessageDispatcher dispatcher;
    private final List<HandlerRegistration> registrations = new CopyOnWriteArrayList<>();
    private final Set<String> activeSubscriptions = ConcurrentHashMap.newKeySet();
    private final ScheduledExecutorService retryScheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "mqtt-subscribe-retry");
        thread.setDaemon(true);
        return thread;
    });

    /**
     * Creates the subscription manager.
     *
     * <p>The manager is <em>not</em> attached to the registry by the
     * constructor: {@link #attach()} must be called once the context is ready
     * so that the wiring stays explicit and testable.
     *
     * @param registry   the client registry this manager listens to
     * @param dispatcher the message dispatcher wired into the subscription callbacks
     */
    public MqttSubscriptionManager(MqttClientRegistry registry, MqttMessageDispatcher dispatcher) {
        this.registry = registry;
        this.dispatcher = dispatcher;
    }

    /**
     * Attaches this manager to its registry so that it is notified of
     * connect/disconnect events. Idempotent.
     */
    public void attach() {
        registry.setClientEventListener(this);
    }

    @Override
    public void onConnected(String serverId, Mqtt5AsyncClient client) {
        subscribePending(serverId, client);
    }

    @Override
    public void onDisconnected(String serverId, Mqtt5AsyncClient client) {
        // Drop the "already subscribed" marks of this server so the next
        // connection re-establishes the subscriptions.
        activeSubscriptions.removeIf(key -> key.startsWith(serverId + "|"));
    }

    /**
     * Registers a handler bean. Topics are subscribed on every client that is
     * already connected; clients that connect later trigger the subscription
     * through the registry's connected event.
     *
     * @param registration the resolved handler registration
     */
    public void registerHandler(HandlerRegistration registration) {
        registrations.add(registration);
        log.info("Registered MQTT message handler '{}' for topics {} (qos={})",
                registration.getBeanName(), registration.getTopics(), registration.getQos());
        registry.allClients().forEach((serverId, client) -> {
            if (registration.targets(serverId) && registry.isConnected(serverId)) {
                subscribeTopics(registration, serverId, client);
            }
        });
    }

    /**
     * Returns the processing statistics of all registered handlers, keyed by
     * bean name.
     *
     * @return the statistics snapshot
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> result = new LinkedHashMap<>();
        for (HandlerRegistration registration : registrations) {
            HandlerStatistics statistics = registration.statistics();
            if (statistics != null) {
                result.put(registration.getBeanName(), statistics.toMap());
            }
        }
        return result;
    }

    /**
     * Returns the topic filters a handler is registered for.
     *
     * @param beanName the bean name of the handler
     * @return the topics (empty when the bean is not a registered handler)
     */
    public List<String> getTopics(String beanName) {
        return registrations.stream()
                .filter(registration -> registration.getBeanName().equals(beanName))
                .flatMap(registration -> registration.getTopics().stream())
                .toList();
    }

    private void subscribePending(String serverId, Mqtt5AsyncClient client) {
        for (HandlerRegistration registration : registrations) {
            if (registration.targets(serverId)) {
                subscribeTopics(registration, serverId, client);
            }
        }
    }

    private void subscribeTopics(HandlerRegistration registration, String serverId, Mqtt5AsyncClient client) {
        for (String topic : registration.getTopics()) {
            String key = subscriptionKey(serverId, registration, topic);
            if (!activeSubscriptions.add(key)) {
                continue; // already subscribed (or a retry is in flight)
            }
            subscribe(registration, serverId, topic, client, 1);
        }
    }

    private void subscribe(HandlerRegistration registration, String serverId, String topic,
                           Mqtt5AsyncClient client, int attempt) {
        try {
            client.subscribeWith()
                    .topicFilter(topic)
                    .qos(registration.getQos())
                    .retainAsPublished(registration.getAnnotation().retainAsPublished())
                    .callback(publish -> {
                        try {
                            dispatcher.dispatch(publish, registration, serverId);
                        } catch (Exception e) {
                            log.error("Error dispatching message on topic '{}' (server '{}')", topic, serverId, e);
                        }
                    })
                    .send()
                    .whenComplete((subAck, error) -> {
                        if (error != null) {
                            handleSubscriptionFailure(registration, serverId, topic, client, attempt, error);
                        } else {
                            log.info("Subscribed handler '{}' to topic '{}' on server {} (reasonCodes={})",
                                    registration.getBeanName(), topic, serverId, subAck.getReasonCodes());
                        }
                    });
        } catch (Exception e) {
            // Synchronous failure (e.g. invalid topic filter) — retrying won't help.
            // The mark must be removed, otherwise every later (re)connect would
            // consider the topic "already subscribed" and silently skip it forever.
            activeSubscriptions.remove(subscriptionKey(serverId, registration, topic));
            log.error("Failed to subscribe handler '{}' to topic '{}' on server {}: {}",
                    registration.getBeanName(), topic, serverId, e.toString());
        }
    }

    private void handleSubscriptionFailure(HandlerRegistration registration, String serverId, String topic,
                                           Mqtt5AsyncClient client, int attempt, Throwable error) {
        String key = subscriptionKey(serverId, registration, topic);
        if (attempt < MAX_SUBSCRIPTION_ATTEMPTS) {
            log.warn("Subscription of handler '{}' to topic '{}' on server {} failed (attempt {}/{}), "
                            + "retrying in {} ms: {}",
                    registration.getBeanName(), topic, serverId, attempt, MAX_SUBSCRIPTION_ATTEMPTS,
                    RETRY_DELAY_MS, error.toString());
            retryScheduler.schedule(
                    () -> subscribe(registration, serverId, topic, client, attempt + 1),
                    RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        } else {
            // Drop the mark so a later (re)connection can retry the subscription.
            activeSubscriptions.remove(key);
            log.error("Giving up on subscription of handler '{}' to topic '{}' on server {} after {} attempts: {}",
                    registration.getBeanName(), topic, serverId, attempt, error.toString());
        }
    }

    private static String subscriptionKey(String serverId, HandlerRegistration registration, String topic) {
        return serverId + "|" + registration.getBeanName() + "|" + topic;
    }

    /**
     * Shuts down the retry scheduler and releases the registration state.
     */
    @Override
    public void destroy() {
        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        registrations.clear();
        activeSubscriptions.clear();
    }
}
