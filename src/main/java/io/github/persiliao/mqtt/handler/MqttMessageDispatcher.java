package io.github.persiliao.mqtt.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import io.github.persiliao.mqtt.MqttMessageHandler;
import io.github.persiliao.mqtt.autoconfigure.properties.MqttProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationContext;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The per-message pipeline: duplicate detection, payload size validation,
 * dispatch (synchronously, on the async pool, or on a per-key ordered
 * executor), handler invocation and statistics.
 *
 * <p>The async pool and the ordered executors use daemon threads and a
 * caller-runs rejection policy, so messages are never silently dropped and
 * worker threads never block JVM exit.
 *
 * @since 3.0.0
 */
public class MqttMessageDispatcher implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MqttMessageDispatcher.class);

    private static final int DEFAULT_ASYNC_QUEUE_CAPACITY = 1024;
    private static final int DEFAULT_ORDERED_QUEUE_CAPACITY = 1024;
    private static final int DEDUPLICATION_WINDOW = 1000;

    private final MqttProperties properties;
    private final ApplicationContext applicationContext;

    private final Map<String, LruSet> deduplicationCaches = new ConcurrentHashMap<>();
    private final Map<String, ExecutorService> orderedExecutors = new ConcurrentHashMap<>();
    private volatile ExecutorService asyncExecutor;
    private volatile ObjectMapper objectMapper;
    private volatile boolean objectMapperMissingLogged;

    /**
     * Creates a dispatcher.
     *
     * @param properties         the MQTT properties (for the async pool sizing)
     * @param applicationContext the context used to lazily resolve the
     *                           {@link ObjectMapper}
     */
    public MqttMessageDispatcher(MqttProperties properties, ApplicationContext applicationContext) {
        this.properties = properties;
        this.applicationContext = applicationContext;
    }

    /**
     * Entry point invoked for every incoming publish matched by a subscription.
     *
     * @param publish      the incoming publish
     * @param registration the handler registration the subscription belongs to
     * @param serverId     the server id the message arrived on
     */
    public void dispatch(Mqtt5Publish publish, HandlerRegistration registration, String serverId) {
        MqttMessageHandler annotation = registration.getAnnotation();

        if (annotation.deduplicate() && isDuplicate(publish, serverId)) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping duplicate message on topic '{}' (server '{}')", topic(publish), serverId);
            }
            return;
        }

        if (annotation.maxPayloadSize() > 0) {
            int size = publish.getPayloadAsBytes().length;
            if (size > annotation.maxPayloadSize()) {
                log.warn("Dropping message on topic '{}' (server '{}'): payload of {} bytes exceeds maxPayloadSize {}",
                        topic(publish), serverId, size, annotation.maxPayloadSize());
                if (registration.statistics() != null) {
                    registration.statistics().recordFailure();
                }
                return;
            }
        }

        ExecutorService executor = executorFor(annotation, publish, serverId, registration);
        Runnable task = () -> runHandler(publish, registration, serverId);
        if (executor != null) {
            executor.execute(task);
        } else {
            // Synchronous mode: run on the MQTT client callback thread.
            task.run();
        }
    }

    /**
     * Runs the handler methods of the registration, first success wins.
     */
    private void runHandler(Mqtt5Publish publish, HandlerRegistration registration, String serverId) {
        HandlerStatistics statistics = registration.statistics();
        long start = System.nanoTime();
        try {
            boolean handled = false;
            for (HandlerMethod method : registration.getMethods()) {
                try {
                    method.invoke(registration.getBean(), publish, serverId, objectMapper());
                    handled = true;
                    break;
                } catch (Exception e) {
                    log.debug("Handler method {} failed for message on topic '{}' (server '{}'): {}",
                            method.getMethod().getName(), topic(publish), serverId, e.toString());
                }
            }
            if (handled) {
                if (log.isDebugEnabled()) {
                    log.debug("Handled message on topic '{}' (server '{}') with '{}'",
                            topic(publish), serverId, registration.getBeanName());
                }
                if (statistics != null) {
                    statistics.recordSuccess(System.nanoTime() - start);
                }
            } else {
                if (statistics != null) {
                    statistics.recordFailure();
                }
                log.warn("No handler method succeeded for message on topic '{}' (server '{}') of handler '{}'",
                        topic(publish), serverId, registration.getBeanName());
            }
        } catch (Exception e) {
            if (statistics != null) {
                statistics.recordFailure();
            }
            log.error("Unexpected error while handling message on topic '{}' (server '{}') of handler '{}'",
                    topic(publish), serverId, registration.getBeanName(), e);
        }
    }

    private ExecutorService executorFor(MqttMessageHandler annotation, Mqtt5Publish publish,
                                        String serverId, HandlerRegistration registration) {
        return switch (annotation.ordering()) {
            case PER_TOPIC -> orderedExecutor(
                    "topic|" + topic(publish) + "|" + registration.getBeanName(), annotation);
            case PER_CLIENT -> orderedExecutor(
                    "client|" + serverId + "|" + registration.getBeanName(), annotation);
            case NONE -> annotation.async() ? asyncExecutor() : null;
        };
    }

    /**
     * A dedicated single-thread executor per key preserves the ordering
     * guarantee while moving processing off the MQTT client thread.
     */
    private ExecutorService orderedExecutor(String key, MqttMessageHandler annotation) {
        return orderedExecutors.computeIfAbsent(key, k -> {
            int capacity = annotation.maxConcurrentMessages() > 0
                    ? annotation.maxConcurrentMessages()
                    : DEFAULT_ORDERED_QUEUE_CAPACITY;
            return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(capacity),
                    daemonThreadFactory("mqtt-ordered-" + k + "-"),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        });
    }

    private ExecutorService asyncExecutor() {
        ExecutorService executor = asyncExecutor;
        if (executor == null) {
            synchronized (this) {
                executor = asyncExecutor;
                if (executor == null) {
                    MqttProperties.AsyncConfig config = properties.getAsync();
                    int processors = Runtime.getRuntime().availableProcessors();
                    int core = config.getCorePoolSize() > 0 ? config.getCorePoolSize() : processors;
                    int max = config.getMaxPoolSize() > 0 ? config.getMaxPoolSize() : Math.max(2 * processors, core);
                    int queue = config.getQueueCapacity() > 0 ? config.getQueueCapacity() : DEFAULT_ASYNC_QUEUE_CAPACITY;
                    executor = new ThreadPoolExecutor(core, max, 60L, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>(queue),
                            daemonThreadFactory("mqtt-async-"),
                            new ThreadPoolExecutor.CallerRunsPolicy());
                    asyncExecutor = executor;
                    log.info("Initialized MQTT async executor: core={}, max={}, queue={}", core, max, queue);
                }
            }
        }
        return executor;
    }

    private boolean isDuplicate(Mqtt5Publish publish, String serverId) {
        LruSet cache = deduplicationCaches.computeIfAbsent(
                serverId + "|" + topic(publish), key -> new LruSet(DEDUPLICATION_WINDOW));
        return !cache.addIfAbsent(fingerprint(publish));
    }

    /**
     * FNV-1a 64-bit fingerprint over topic, QoS, retain flag and payload.
     */
    private static long fingerprint(Mqtt5Publish publish) {
        long hash = 0xcbf29ce484222325L;
        byte[] topic = topic(publish).getBytes(StandardCharsets.UTF_8);
        for (byte b : topic) {
            hash ^= b;
            hash *= 0x100000001b3L;
        }
        hash ^= (byte) publish.getQos().getCode();
        hash *= 0x100000001b3L;
        hash ^= (byte) (publish.isRetain() ? 1 : 0);
        hash *= 0x100000001b3L;
        byte[] payload = publish.getPayloadAsBytes();
        for (byte b : payload) {
            hash ^= b;
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private ObjectMapper objectMapper() {
        ObjectMapper mapper = objectMapper;
        if (mapper == null) {
            synchronized (this) {
                mapper = objectMapper;
                if (mapper == null) {
                    try {
                        mapper = applicationContext.getBean(ObjectMapper.class);
                    } catch (Exception e) {
                        if (!objectMapperMissingLogged) {
                            log.warn("No ObjectMapper bean is available; JSON deserialization of MQTT "
                                    + "payloads is disabled. Add an ObjectMapper bean (e.g. via "
                                    + "spring-boot-starter-json) to enable it.");
                            objectMapperMissingLogged = true;
                        }
                        return null;
                    }
                    objectMapper = mapper;
                }
            }
        }
        return mapper;
    }

    /**
     * Shuts down the executors owned by this dispatcher.
     */
    @Override
    public void destroy() {
        shutdownExecutor(asyncExecutor);
        orderedExecutors.values().forEach(MqttMessageDispatcher::shutdownExecutor);
        orderedExecutors.clear();
    }

    private static void shutdownExecutor(ExecutorService executor) {
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory daemonThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, prefix + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static String topic(Mqtt5Publish publish) {
        return publish.getTopic().toString();
    }
}
