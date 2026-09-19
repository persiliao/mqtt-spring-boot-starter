package io.github.persiliao.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Enhanced MQTT Message Handler Processor
 * <p>
 * This processor automatically registers message handlers for beans annotated with @MqttMessageHandler.
 * It supports all the enhanced features defined in the @MqttMessageHandler annotation, including:
 * - Automatic topic subscription
 * - Message validation and deserialization
 * - Ordered message processing
 * - Statistics collection
 * - Deduplication
 *
 * @see MqttMessageHandler
 */
@Slf4j
@RequiredArgsConstructor
public class MqttMessageHandlerProcessor implements BeanPostProcessor, ApplicationContextAware, DisposableBean {

    // Dependencies
    private ApplicationContext applicationContext;
    // Internal state management
    private final Map<String, SubscriptionContext> subscriptionContexts = new ConcurrentHashMap<>();
    private final Map<Class<?>, List<EnhancedHandlerMethod>> handlerMethodCache = new ConcurrentHashMap<>();
    private final Map<String, ExecutorService> orderedExecutors = new ConcurrentHashMap<>();
    private final Map<String, LimitedSizeSet<Integer>> processedMessages = new ConcurrentHashMap<>();
    private final Map<String, HandlerStatistics> handlerStatistics = new ConcurrentHashMap<>();
    private final Map<String, List<Class<?>>> handlerGroups = new ConcurrentHashMap<>();

    // Dedicated executor for async (off-MQTT-thread) message processing.
    // Initialized lazily on first use (see getAsyncExecutor) so that the
    // MqttProperties bean is NOT pulled in during this BeanPostProcessor's own
    // creation, which would instantiate it before the ConfigurationProperties
    // binding post-processor has registered and silently leave config unbound.
    private volatile ExecutorService asyncExecutor;

    // Configuration constants
    private static final int MAX_SUBSCRIPTION_RETRIES = 3;
    private static final long SUBSCRIPTION_RETRY_DELAY_MS = 2000;
    private static final int DEFAULT_MAX_CONCURRENT_MESSAGES = Runtime.getRuntime().availableProcessors() * 2;
    private static final int MAX_PROCESSED_MESSAGES_CACHE = 1000;
    private static final int DEFAULT_ASYNC_CORE_POOL = Runtime.getRuntime().availableProcessors();
    private static final int DEFAULT_ASYNC_MAX_POOL = Runtime.getRuntime().availableProcessors() * 2;
    private static final int DEFAULT_ASYNC_QUEUE_CAPACITY = 1024;

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    private ObjectMapper getObjectMapper() {
        return getBeanSafely(ObjectMapper.class);
    }

    private MqttProperties getMqttProperties() {
        return getBeanSafely(MqttProperties.class);
    }

    /**
     * Builds the dedicated async executor used to process messages off the
     * MQTT client thread when a handler is declared with async = true.
     * <p>
     * Pool sizing is configurable via {@code mqtt.async.*}; a value of 0 uses a
     * sensible default. The executor uses a daemon thread factory and a
     * CallerRunsPolicy as backpressure so messages are never silently dropped.
     */
    /**
     * Returns the async executor, creating it on first access.
     * <p>
     * Initialization is deliberately deferred until the first message is
     * dispatched (runtime, after the application context is fully refreshed)
     * rather than performed in a {@code @PostConstruct}. Performing it eagerly
     * would force the {@code MqttProperties} bean to be instantiated while this
     * {@link BeanPostProcessor} is itself being created — before the
     * {@code ConfigurationPropertiesBindingPostProcessor} has registered — which
     * leaves the {@code @ConfigurationProperties} binding unapplied and silently
     * drops external configuration.
     *
     * @return the async executor, or {@code null} if it could not be created
     */
    private ExecutorService getAsyncExecutor() {
        ExecutorService executor = asyncExecutor;
        if (executor == null) {
            synchronized (this) {
                executor = asyncExecutor;
                if (executor == null) {
                    asyncExecutor = executor = createAsyncExecutor();
                }
            }
        }
        return executor;
    }

    /**
     * Builds the dedicated async executor used to process messages off the
     * MQTT client thread when a handler is declared with async = true.
     * <p>
     * Pool sizing is configurable via {@code mqtt.async.*}; a value of 0 uses a
     * sensible default. The executor uses a daemon thread factory and a
     * CallerRunsPolicy as backpressure so messages are never silently dropped.
     */
    private ExecutorService createAsyncExecutor() {
        MqttProperties.AsyncConfig cfg = null;
        MqttProperties props = getMqttProperties();
        if (props != null && props.getAsync() != null) {
            cfg = props.getAsync();
        }

        int core = (cfg != null && cfg.getCorePoolSize() > 0) ? cfg.getCorePoolSize() : DEFAULT_ASYNC_CORE_POOL;
        int max = (cfg != null && cfg.getMaxPoolSize() > 0) ? cfg.getMaxPoolSize() : DEFAULT_ASYNC_MAX_POOL;
        int queue = (cfg != null && cfg.getQueueCapacity() > 0) ? cfg.getQueueCapacity() : DEFAULT_ASYNC_QUEUE_CAPACITY;

        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(core);
        executor.setMaxPoolSize(max);
        executor.setQueueCapacity(queue);
        executor.setThreadNamePrefix("mqtt-async-");
        executor.setThreadFactory(new DaemonThreadFactory("mqtt-async-"));
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();

        log.info("Initialized MQTT async message executor: core={}, max={}, queue={}", core, max, queue);
        return executor.getThreadPoolExecutor();
    }

    /**
     * Enhanced subscription context with all necessary information
     */
    @Data
    @AllArgsConstructor
    @Builder
    private static class SubscriptionContext {
        /**
         * Current state of the subscription
         */
        @Builder.Default
        private AtomicReference<SubscriptionState> state = new AtomicReference<>(SubscriptionState.PENDING);

        /**
         * Number of subscription retry attempts
         */
        @Builder.Default
        private AtomicInteger retryCount = new AtomicInteger(0);

        /**
         * Unique identifier for this subscription
         */
        private String subscriptionKey;

        /**
         * MQTT topic pattern
         */
        private String topic;

        /**
         * Server identifier (empty for default server)
         */
        private String serverId;

        /**
         * MQTT client instance
         */
        private Mqtt5AsyncClient client;

        /**
         * Handler bean instance
         */
        private Object handlerBean;

        /**
         * Annotation configuration
         */
        private MqttMessageHandler annotation;

        /**
         * Timestamp of last subscription attempt
         */
        @Builder.Default
        private long lastAttemptTimestamp = System.currentTimeMillis();

        /**
         * Attempts to transition to a new subscription state
         *
         * @param newState Target state to transition to
         * @return true if transition was successful, false otherwise
         */
        public boolean transitionTo(SubscriptionState newState) {
            SubscriptionState current = state.get();

            switch (newState) {
                case PENDING:
                    return state.compareAndSet(SubscriptionState.FAILED, SubscriptionState.PENDING);
                case SUBSCRIBED:
                    return state.compareAndSet(SubscriptionState.PENDING, SubscriptionState.SUBSCRIBED);
                case FAILED:
                    return state.compareAndSet(SubscriptionState.PENDING, SubscriptionState.FAILED);
                case UNSUBSCRIBED:
                    return state.compareAndSet(SubscriptionState.SUBSCRIBED, SubscriptionState.UNSUBSCRIBED);
                default:
                    return false;
            }
        }

        /**
         * Increments the retry count
         */
        public void incrementRetry() {
            retryCount.incrementAndGet();
            lastAttemptTimestamp = System.currentTimeMillis();
        }

        /**
         * Gets the current retry count
         */
        public int getRetryCount() {
            return retryCount.get();
        }

        /**
         * Gets the AtomicInteger instance for retry count
         */
        public AtomicInteger getRetryCountAtomic() {
            return retryCount;
        }

        /**
         * Checks if this subscription should be retried
         *
         * @return true if retry is allowed, false otherwise
         */
        public boolean shouldRetry() {
            return retryCount.get() < MAX_SUBSCRIPTION_RETRIES && state.get() == SubscriptionState.FAILED;
        }

        /**
         * Gets the age of the last subscription attempt
         *
         * @return Time in milliseconds since last attempt
         */
        public long getAttemptAge() {
            return System.currentTimeMillis() - lastAttemptTimestamp;
        }
    }

    /**
     * Schedules a retry for failed subscription
     *
     * @param context Subscription context
     */
    private void scheduleRetry(SubscriptionContext context) {
        if (!context.shouldRetry()) {
            return;
        }

        context.incrementRetry();
        context.transitionTo(SubscriptionState.PENDING);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "mqtt-subscribe-retry-" + context.getTopic());
            t.setDaemon(true);
            return t;
        });

        scheduler.schedule(() -> {
            try {
                log.info("Retrying subscription to topic {} on server {} (attempt {}/{})", context.getTopic(), context.getServerId(), context.getRetryCount(), MAX_SUBSCRIPTION_RETRIES);
                doSubscribe(context);
            } finally {
                scheduler.shutdown();
            }
        }, SUBSCRIPTION_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * Subscription state enumeration
     */
    private enum SubscriptionState {
        /**
         * Subscription is being established
         */
        PENDING,

        /**
         * Successfully subscribed and active
         */
        SUBSCRIBED,

        /**
         * Subscription failed
         */
        FAILED,

        /**
         * Previously subscribed, now unsubscribed
         */
        UNSUBSCRIBED
    }

    /**
     * Enhanced handler method with validation and deserialization capabilities
     */
    @Data
    @AllArgsConstructor
    @Builder
    private static class EnhancedHandlerMethod {
        /**
         * Reflection method instance
         */
        private Method method;

        /**
         * Bean instance containing the method
         */
        private Object bean;

        /**
         * Parameter resolvers for method invocation
         */
        private ParameterResolver[] parameterResolvers;

        /**
         * Whether to auto-deserialize payload
         */
        private boolean autoDeserialize;

        /**
         * Expected content type of payload
         */
        private String contentType;

        /**
         * Maximum allowed payload size in bytes
         */
        private int maxPayloadSize;

        /**
         * Whether to validate incoming messages
         */
        private boolean validation;

        /**
         * Method priority for execution order
         */
        private int priority;

        /**
         * Handler group identifier
         */
        private String group;

        /**
         * ObjectMapper for JSON deserialization
         */
        private ObjectMapper objectMapper;

        /**
         * Invokes the handler method with the given message
         *
         * @param publish    MQTT publish message
         * @param serverId   Server identifier
         * @param statistics Statistics tracker
         * @return Result of method invocation
         */
        public Object invoke(Mqtt5Publish publish, String serverId, HandlerStatistics statistics) {
            if (!validateMessage(publish)) {
                throw new IllegalArgumentException("Message validation failed for topic: " + publish.getTopic());
            }

            Object[] args = new Object[parameterResolvers.length];

            for (int i = 0; i < parameterResolvers.length; i++) {
                args[i] = parameterResolvers[i].resolve(publish, serverId, this);
            }

            return ReflectionUtils.invokeMethod(method, bean, args);
        }


        /**
         * Validates the incoming message according to configuration
         *
         * @param publish MQTT publish message
         * @return true if message is valid, false otherwise
         */
        private boolean validateMessage(Mqtt5Publish publish) {
            if (!validation) {
                return true;
            }

            // Validate payload size
            if (maxPayloadSize > 0) {
                byte[] payload = publish.getPayloadAsBytes();
                if (payload.length > maxPayloadSize) {
                    log.warn("Message payload size {} exceeds maximum {} bytes for topic {}", payload.length, maxPayloadSize, publish.getTopic());
                    return false;
                }
            }

            return true;
        }

        /**
         * Creates parameter resolvers for the handler method
         *
         * @param method Reflection method
         * @return Array of parameter resolvers
         */
        private ParameterResolver[] createParameterResolvers(Method method) {
            Class<?>[] paramTypes = method.getParameterTypes();
            ParameterResolver[] resolvers = new ParameterResolver[paramTypes.length];

            for (int i = 0; i < paramTypes.length; i++) {
                resolvers[i] = createParameterResolver(paramTypes[i], i, paramTypes.length);
            }

            return resolvers;
        }


        /**
         * Creates a parameter resolver for a specific parameter type
         *
         * @param paramType   Parameter class type
         * @param paramIndex  Parameter index in method signature
         * @param totalParams Total number of parameters
         * @return Parameter resolver instance
         */
        private ParameterResolver createParameterResolver(Class<?> paramType, int paramIndex, int totalParams) {
            if (paramType == String.class && paramIndex == 0) {
                return (publish, serverId, handler) -> publish.getTopic().toString();
            } else if (paramType == byte[].class) {
                return (publish, serverId, handler) -> publish.getPayloadAsBytes();
            } else if (paramType == Mqtt5Publish.class) {
                return (publish, serverId, handler) -> publish;
            } else if (paramType == Map.class && autoDeserialize && "application/json".equals(contentType)) {
                return (publish, serverId, handler) -> deserializeToMap(publish);
            } else if (paramType.isArray() && paramType.getComponentType() == byte.class) {
                return (publish, serverId, handler) -> publish.getPayloadAsBytes();
            } else {
                return ((publish, serverId, handler) -> deserializePayload(publish, paramType, serverId, handler));
            }
        }

        /**
         * Deserializes MQTT payload based on content type
         *
         * @param publish MQTT publish message
         * @param handler Handler configuration
         * @return Deserialized payload
         */
        private Object deserializePayload(Mqtt5Publish publish, Class<?> paramType, String serverId, EnhancedHandlerMethod handler) {
            byte[] payload = publish.getPayloadAsBytes();
            if (payload.length == 0) {
                return null;
            }

            if ("application/json".equals(handler.getContentType())) {
                if (autoDeserialize && handler.getObjectMapper() != null) {
                    try {
                        return handler.getObjectMapper().readValue(payload, paramType);
                    } catch (Exception e) {
                        log.warn("Failed to deserialize JSON payload for topic {}", publish.getTopic(), e);
                        return new String(payload, StandardCharsets.UTF_8);
                    }
                } else {
                    log.warn("ObjectMapper not available, cannot deserialize JSON for topic {}", publish.getTopic());
                    return new String(payload, StandardCharsets.UTF_8);
                }
            } else if ("text/plain".equals(handler.getContentType())) {
                return new String(payload, StandardCharsets.UTF_8);
            } else {
                return new String(payload, StandardCharsets.UTF_8);
            }
        }

        /**
         * Deserializes JSON payload to Map
         *
         * @param publish MQTT publish message
         * @return Deserialized map
         */
        private Map<String, Object> deserializeToMap(Mqtt5Publish publish) {
            byte[] payload = publish.getPayloadAsBytes();
            if (payload.length == 0) {
                return Collections.emptyMap();
            }

            try {
                return this.objectMapper.readValue(payload, Map.class);
            } catch (Exception e) {
                log.warn("Failed to deserialize payload to Map for topic {}", publish.getTopic(), e);
                return Collections.emptyMap();
            }
        }


        @FunctionalInterface
        private interface ParameterResolver {
            /**
             * Resolves parameter value for method invocation
             *
             * @param publish  MQTT publish message
             * @param serverId Server identifier
             * @param handler  Handler method metadata
             * @return Resolved parameter value
             */
            Object resolve(Mqtt5Publish publish, String serverId, EnhancedHandlerMethod handler);
        }
    }

    /**
     * Statistics collector for handler performance monitoring
     */
    @Data
    @AllArgsConstructor
    @Builder
    private static class HandlerStatistics {
        /**
         * Total messages processed
         */
        @Builder.Default
        private AtomicLong totalMessages = new AtomicLong(0);

        /**
         * Total successful message processings
         */
        @Builder.Default
        private AtomicLong successCount = new AtomicLong(0);

        /**
         * Total failed message processings
         */
        @Builder.Default
        private AtomicLong failureCount = new AtomicLong(0);

        /**
         * Total processing time in nanoseconds
         */
        @Builder.Default
        private AtomicLong totalProcessingTime = new AtomicLong(0);

        /**
         * Timestamp of last message processing
         */
        @Builder.Default
        private AtomicLong lastProcessedAt = new AtomicLong(0);

        /**
         * Maximum processing time in nanoseconds
         */
        @Builder.Default
        private AtomicLong maxProcessingTime = new AtomicLong(0);

        /**
         * Minimum processing time in nanoseconds
         */
        @Builder.Default
        private AtomicLong minProcessingTime = new AtomicLong(Long.MAX_VALUE);

        /**
         * Start time for throughput calculation
         */
        @Builder.Default
        private long startTime = System.currentTimeMillis();

        /**
         * Records a successful message processing
         *
         * @param processingTime Processing time in nanoseconds
         */
        public void recordSuccess(long processingTime) {
            totalMessages.incrementAndGet();
            successCount.incrementAndGet();
            totalProcessingTime.addAndGet(processingTime);
            lastProcessedAt.set(System.currentTimeMillis());

            maxProcessingTime.set(Math.max(maxProcessingTime.get(), processingTime));
            minProcessingTime.set(Math.min(minProcessingTime.get(), processingTime));
        }

        /**
         * Records a failed message processing
         */
        public void recordFailure() {
            totalMessages.incrementAndGet();
            failureCount.incrementAndGet();
            lastProcessedAt.set(System.currentTimeMillis());
        }

        /**
         * Gets average processing time in milliseconds
         *
         * @return Average processing time
         */
        public double getAverageProcessingTimeMs() {
            long total = totalMessages.get();
            if (total == 0) {
                return 0.0;
            }
            return totalProcessingTime.get() / (double) total / 1_000_000;
        }

        /**
         * Gets throughput in messages per second
         *
         * @return Throughput in messages/second
         */
        public double getThroughput() {
            long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
            if (elapsedSeconds == 0) {
                return 0.0;
            }
            return totalMessages.get() / (double) elapsedSeconds;
        }

        /**
         * Gets success rate percentage
         *
         * @return Success rate percentage
         */
        public double getSuccessRate() {
            long total = totalMessages.get();
            if (total == 0) {
                return 0.0;
            }
            return (successCount.get() * 100.0) / total;
        }
    }

    /**
     * Limited size set for message deduplication
     */
    @Data
    @AllArgsConstructor
    private static class LimitedSizeSet<T> {
        /**
         * Maximum number of elements
         */
        private final int maxSize;

        /**
         * Internal storage
         */
        private final Set<T> storage = new LinkedHashSet<T>() {

            private boolean removeEldestEntry(Map.Entry<T, Boolean> eldest) {
                return size() > maxSize;
            }
        };

        /**
         * Adds an element to the set
         *
         * @param element Element to add
         * @return true if element was added, false if already present
         */
        public boolean add(T element) {
            synchronized (storage) {
                return storage.add(element);
            }
        }

        /**
         * Checks if set contains element
         *
         * @param element Element to check
         * @return true if element exists, false otherwise
         */
        public boolean contains(T element) {
            synchronized (storage) {
                return storage.contains(element);
            }
        }

        /**
         * Gets current size of the set
         *
         * @return Number of elements in set
         */
        public int size() {
            synchronized (storage) {
                return storage.size();
            }
        }
    }

    @Override
    public Object postProcessAfterInitialization(@NotNull Object bean, @NotNull String beanName) throws BeansException {
        if (getMqttProperties() == null || !getMqttProperties().isEnabled()) {
            if (log.isDebugEnabled()) {
                log.debug("MQTT is disabled or properties not available, skipping handler registration");
            }
            return bean;
        }

        Class<?> targetClass = AopProxyUtils.ultimateTargetClass(bean);
        MqttMessageHandler annotation = AnnotationUtils.findAnnotation(targetClass, MqttMessageHandler.class);

        if (annotation != null) {
            registerMessageHandler(bean, annotation);
        }

        return bean;
    }

    /**
     * Registers a message handler based on annotation configuration
     *
     * @param bean       Handler bean instance
     * @param annotation MqttMessageHandler annotation
     */
    private void registerMessageHandler(Object bean, MqttMessageHandler annotation) {
        try {
            // Validate topics
            String[] topics = annotation.topics();
            if (topics == null || topics.length == 0) {
                log.warn("No topics specified for MqttMessageHandler: {}", bean.getClass().getName());
                return;
            }

            // Validate QoS
            int qos = annotation.qos();
            if (qos < 0 || qos > 2) {
                log.error("Invalid QoS value {} for MqttMessageHandler: {}, using default QoS 0", qos, bean.getClass().getName());
                qos = 0;
            }

            // Resolve target server IDs
            List<String> targetServerIds = resolveServerIds(annotation.serverIds(), annotation.serverId());
            if (targetServerIds.isEmpty()) {
                log.warn("No target servers found for MqttMessageHandler: {}", bean.getClass().getName());
                return;
            }

            // Cache handler methods
            cacheHandlerMethods(bean, annotation);

            // Register handler in group
            registerHandlerGroup(bean.getClass(), annotation.group());

            // Subscribe to topics for each server
            for (String serverId : targetServerIds) {
                List<Mqtt5AsyncClient> clients = getClients(serverId);

                if (clients.isEmpty()) {
                    log.warn("No MQTT clients found for serverId: {} for handler: {}", serverId, bean.getClass().getName());
                    continue;
                }

                for (Mqtt5AsyncClient client : clients) {
                    for (String topic : topics) {
                        subscribeToTopic(client, serverId, topic, qos, annotation, bean);
                    }
                }
            }

            log.info("Registered MQTT message handler for topics {} on servers {} for bean: {}", Arrays.toString(topics), targetServerIds, bean.getClass().getName());

        } catch (Exception e) {
            log.error("Failed to register MQTT message handler for bean: {}", bean.getClass().getName(), e);
        }
    }

    /**
     * Safely gets a bean from application context, returns null if not available
     *
     * @param beanName Bean name
     * @param beanType Bean type
     * @return Bean instance or null
     */
    private <T> T getBeanSafely(String beanName, Class<T> beanType) {
        try {
            return applicationContext.getBean(beanName, beanType);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Bean {} of type {} not available yet: {}", beanName, beanType.getSimpleName(), e.getMessage());
            }
            return null;
        }
    }

    /**
     * Safely gets a bean from application context, returns null if not available
     *
     * @param beanType Bean type
     * @return Bean instance or null
     */
    private <T> T getBeanSafely(Class<T> beanType) {
        try {
            return applicationContext.getBean(beanType);
        } catch (Exception e) {
            if (log.isDebugEnabled()) {
                log.debug("Bean type {} not available yet: {}", beanType.getSimpleName(), e.getMessage());
            }
            return null;
        }
    }

    /**
     * Resolves server IDs from annotation configuration
     *
     * @param serverIds      Array of server IDs
     * @param singleServerId Single server ID
     * @return List of resolved server IDs
     */
    private List<String> resolveServerIds(String[] serverIds, String singleServerId) {
        List<String> targetServerIds = new ArrayList<>();

        if (serverIds != null && serverIds.length > 0) {
            Collections.addAll(targetServerIds, serverIds);
        } else if (StringUtils.hasText(singleServerId)) {
            targetServerIds.add(singleServerId.trim());
        } else {
            // Default: all servers
            targetServerIds.addAll(getAllServerIds());
        }

        return targetServerIds.stream().filter(StringUtils::hasText).distinct().collect(Collectors.toList());
    }

    /**
     * Gets all available server IDs
     *
     * @return List of all server IDs
     */
    private List<String> getAllServerIds() {
        List<String> serverIds = new ArrayList<>();

        try {
            if (getMqttProperties().getMode() == MqttProperties.Mode.SINGLE) {
                Mqtt5AsyncClient singleMqttClient = getBeanSafely(BeanConstants.SINGLE_MQTT_CLIENT, Mqtt5AsyncClient.class);
                if (singleMqttClient != null) {
                    serverIds.add("*");
                }
            } else {
                Map<String, Mqtt5AsyncClient> allClients = getBeanSafely(BeanConstants.MULTI_MQTT_CLIENTS, Map.class);
                if (allClients != null) {
                    serverIds.addAll(allClients.keySet());
                }
            }
        } catch (Exception e) {
            log.warn("Error getting all server IDs", e);
        }

        return serverIds;
    }

    /**
     * Gets MQTT clients for the specified server ID
     *
     * @param serverId Server identifier
     * @return List of MQTT clients
     */
    private List<Mqtt5AsyncClient> getClients(String serverId) {
        List<Mqtt5AsyncClient> clients = new ArrayList<>();

        try {
            MqttProperties.Mode runMode = getMqttProperties().getMode();
            if (MqttProperties.Mode.SINGLE.equals(runMode)) {
                // Single mode or default server
                Mqtt5AsyncClient client = getBeanSafely(BeanConstants.SINGLE_MQTT_CLIENT, Mqtt5AsyncClient.class);
                if (client != null) {
                    clients.add(client);
                }
            }
            if (MqttProperties.Mode.MULTI.equals(runMode)) {
                // noinspection unchecked
                Map<String, Mqtt5AsyncClient> allClients = getBeanSafely(BeanConstants.MULTI_MQTT_CLIENTS, Map.class);

                if (allClients == null) {
                    log.warn("Multi-server clients bean not found");
                    return clients;
                }

                if ("*".equals(serverId.trim())) {
                    // All servers
                    clients.addAll(allClients.values());
                } else if (serverId.contains(",")) {
                    // Multiple specific servers
                    String[] ids = serverId.split(",");
                    for (String id : ids) {
                        Mqtt5AsyncClient client = allClients.get(id.trim());
                        if (client != null) {
                            clients.add(client);
                        } else {
                            log.warn("MQTT client for serverId '{}' not found", id.trim());
                        }
                    }
                } else {
                    // Single specific server
                    Mqtt5AsyncClient client = allClients.get(serverId.trim());
                    if (client != null) {
                        clients.add(client);
                    } else {
                        log.warn("MQTT client for serverId '{}' not found", serverId);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error getting MQTT clients for serverId: {}", serverId, e);
        }

        return clients;
    }

    /**
     * Caches handler methods for the bean
     *
     * @param bean       Handler bean instance
     * @param annotation Annotation configuration
     */
    private void cacheHandlerMethods(Object bean, MqttMessageHandler annotation) {
        Class<?> beanClass = bean.getClass();
        if (handlerMethodCache.containsKey(beanClass)) {
            return;
        }

        List<EnhancedHandlerMethod> methods = new ArrayList<>();
        Method[] allMethods = beanClass.getMethods();

        for (Method method : allMethods) {
            if (isHandlerMethod(method)) {
                // @formatter:off
                EnhancedHandlerMethod enhancedMethod = EnhancedHandlerMethod.builder()
                        .method(method)
                        .bean(bean)
                        .autoDeserialize(annotation.autoDeserialize())
                        .contentType(annotation.contentType())
                        .maxPayloadSize(annotation.maxPayloadSize())
                        .validation(annotation.validation())
                        .priority(annotation.priority())
                        .group(annotation.group())
                        .objectMapper(getObjectMapper())
                        .build();

                enhancedMethod.setParameterResolvers(enhancedMethod.createParameterResolvers(method));
                methods.add(enhancedMethod);
            }
        }

        if (!methods.isEmpty()) {
            handlerMethodCache.put(beanClass, methods);
            if(log.isDebugEnabled()) {
                log.debug("Cached {} handler methods for bean: {}", methods.size(), beanClass.getName());
            }
        }
    }


    /**
     * Checks if a method is a valid handler method
     *
     * @param method Reflection method
     * @return true if method is a valid handler, false otherwise
     */
    private boolean isHandlerMethod(Method method) {
        // Skip static methods
        if (Modifier.isStatic(method.getModifiers())) {
            return false;
        }

        // Check method name
        String methodName = method.getName();
        if (!("handleMessage".equals(methodName) || methodName.startsWith("handle"))) {
            return false;
        }

        // Check parameter count
        return method.getParameterCount() >= 1;
    }

    /**
     * Registers handler in a group
     *
     * @param handlerClass Handler class
     * @param groupName    Group name
     */
    private void registerHandlerGroup(Class<?> handlerClass, String groupName) {
        String group = StringUtils.hasText(groupName) ? groupName : "default";
        handlerGroups.computeIfAbsent(group, k -> new ArrayList<>()).add(handlerClass);
        if(log.isDebugEnabled()) {
            log.debug("Registered handler {} in group {}", handlerClass.getName(), group);
        }
    }

    /**
     * Subscribes to a topic on the specified client
     *
     * @param client MQTT client
     * @param serverId Server identifier
     * @param topic Topic pattern
     * @param qos Quality of Service level
     * @param annotation Annotation configuration
     * @param handlerBean Handler bean instance
     */
    private void subscribeToTopic(Mqtt5AsyncClient client, String serverId, String topic, int qos, MqttMessageHandler annotation, Object handlerBean) {
        String subscriptionKey = generateSubscriptionKey(client, serverId, topic, handlerBean);

        // Check existing subscription
        SubscriptionContext existingContext = subscriptionContexts.get(subscriptionKey);
        if (existingContext != null && SubscriptionState.SUBSCRIBED.equals(existingContext.getState().get())) {
            if(log.isDebugEnabled()) {
                log.debug("Already subscribed to topic {} on server {}", topic, serverId);
            }
            return;
        }

        // Create or update subscription context
        SubscriptionContext context = SubscriptionContext.builder().subscriptionKey(subscriptionKey).topic(topic).serverId(serverId).client(client).handlerBean(handlerBean).annotation(annotation).build();

        subscriptionContexts.put(subscriptionKey, context);

        // Perform subscription
        doSubscribe(context);
    }

    /**
     * Generates a unique subscription key
     *
     * @param client MQTT client
     * @param serverId Server identifier
     * @param topic Topic pattern
     * @param handlerBean Handler bean
     * @return Unique subscription key
     */
    private String generateSubscriptionKey(Mqtt5AsyncClient client, String serverId, String topic, Object handlerBean) {
        return String.format("%s-%s-%s-%s", serverId != null ? serverId : "", Integer.toHexString(System.identityHashCode(client)), topic.hashCode(), handlerBean.getClass().getName().hashCode());
    }

    /**
     * Performs the actual subscription
     *
     * @param context Subscription context
     */
    private void doSubscribe(SubscriptionContext context) {
        try {
            MqttQos mqttQos = MqttQos.fromCode(context.getAnnotation().qos());
            if (mqttQos == null) {
                throw new IllegalArgumentException("Invalid QoS code: " + context.getAnnotation().qos());
            }

            context.getClient().subscribeWith().topicFilter(context.getTopic()).qos(mqttQos).noLocal(false).retainAsPublished(context.getAnnotation().retain()).callback(publish -> handleIncomingMessage(publish, context)).send().whenComplete((subAck, throwable) -> handleSubscriptionResult(subAck, throwable, context));
            if(log.isDebugEnabled()) {
                log.debug("Initiating subscription to topic {} on server {} with QoS {}", context.getTopic(), context.getServerId(), context.getAnnotation().qos());
            }

        } catch (Exception e) {
            log.error("Error initiating subscription to topic {} on server {}: {}", context.getTopic(), context.getServerId(), e.getMessage(), e);
            context.transitionTo(SubscriptionState.FAILED);
            scheduleRetry(context);
        }
    }

    /**
     * Handles incoming MQTT message
     *
     * @param publish MQTT publish message
     * @param context Subscription context
     */
    private void handleIncomingMessage(Mqtt5Publish publish, SubscriptionContext context) {
        try {
            int packetId = getPacketIdentifierWithFallback(publish);
            String messageKey = context.getSubscriptionKey() + "-" + packetId;

            // Check for duplicate messages
            if (context.getAnnotation().deduplicate()) {
                LimitedSizeSet<Integer> messageSet = processedMessages.computeIfAbsent(context.getSubscriptionKey(), k -> new LimitedSizeSet<>(MAX_PROCESSED_MESSAGES_CACHE));

                if (!messageSet.add(packetId)) {
                    if(log.isDebugEnabled()) {
                        log.debug("Skipping duplicate message with packet ID {} for topic {}", packetId, context.getTopic());
                    }
                    return;
                }
            }

            // Get or create statistics tracker
            HandlerStatistics statistics = handlerStatistics.computeIfAbsent(context.getSubscriptionKey(), k -> HandlerStatistics.builder().build());

            // Handle message based on configuration
            MqttMessageHandler.Ordering ordering = context.getAnnotation().ordering();

            if (ordering != MqttMessageHandler.Ordering.NONE) {
                // Ordered processing
                handleMessageWithOrdering(publish, context, statistics, ordering);
            } else {
                // Unordered processing
                handleMessageWithoutOrdering(publish, context, statistics);
            }

        } catch (Exception e) {
            log.error("Error in handleIncomingMessage for topic {}: {}", publish.getTopic(), e.getMessage(), e);
        }
    }

    /**
     * Simpler method for packet identifier
     *
     * @param publish MQTT publish message
     * @return Packet identifier based on message hash
     */
    private int getPacketIdentifierSimple(Mqtt5Publish publish) {
        try {
            String key = publish.getTopic() + "_" + Arrays.hashCode(publish.getPayloadAsBytes()) + "_" + publish.getQos().getCode() + "_" + publish.isRetain();

            return Math.abs(key.hashCode() % 65536); // 返回0-65535范围内的值
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Gets packet identifier with fallback methods
     *
     * @param publish MQTT publish message
     * @return Packet identifier
     */
    private int getPacketIdentifierWithFallback(Mqtt5Publish publish) {
        return getPacketIdentifierSimple(publish);
    }

    /**
     * Updated method to use the correct packet identifier retrieval
     */
    private void handleIncomingMessageFixed(Mqtt5Publish publish, SubscriptionContext context) {
        try {
            // 使用正确的包标识符获取方法
            int packetId = getPacketIdentifierWithFallback(publish);

            // 其余代码保持不变
            String messageKey = context.getSubscriptionKey() + "-" + packetId;

            // Check for duplicate messages
            if (context.getAnnotation().deduplicate()) {
                LimitedSizeSet<Integer> messageSet = processedMessages.computeIfAbsent(context.getSubscriptionKey(), k -> new LimitedSizeSet<>(MAX_PROCESSED_MESSAGES_CACHE));

                if (!messageSet.add(packetId)) {
                    log.debug("Skipping duplicate message for topic {} with generated ID {}", context.getTopic(), packetId);
                    return;
                }
            }

            // 其余处理逻辑...

        } catch (Exception e) {
            log.error("Error in handleIncomingMessage for topic {}: {}", publish.getTopic(), e.getMessage(), e);
        }
    }


    /**
     * Handles message with ordering configuration
     *
     * @param publish    MQTT publish message
     * @param context    Subscription context
     * @param statistics Statistics tracker
     * @param ordering   Ordering configuration
     */
    private void handleMessageWithOrdering(Mqtt5Publish publish, SubscriptionContext context, HandlerStatistics statistics, MqttMessageHandler.Ordering ordering) {
        try {
            String executorKey = getExecutorKey(ordering, context, publish);
            ExecutorService orderedExecutor = getOrderedExecutor(executorKey, context.getAnnotation());

            if (orderedExecutor != null) {
                // A dedicated single-thread executor already runs the task off the
                // MQTT thread while preserving per-key order; no async re-dispatch.
                orderedExecutor.submit(() -> safeRun(publish, context, statistics));
            } else {
                // Ordering requested but no concurrency limit: process inline on
                // the calling thread so order is still preserved on a single thread.
                handleMessageCore(publish, context, statistics);
            }
        } catch (Exception e) {
            log.error("Error in ordered message processing for topic {}: {}", publish.getTopic(), e.getMessage(), e);
        }
    }

    /**
     * Handles message without ordering configuration
     *
     * @param publish    MQTT publish message
     * @param context    Subscription context
     * @param statistics Statistics tracker
     */
    private void handleMessageWithoutOrdering(Mqtt5Publish publish, SubscriptionContext context, HandlerStatistics statistics) {
        dispatch(publish, context, statistics);
    }

    /**
     * Gets executor key based on ordering configuration
     *
     * @param ordering Ordering configuration
     * @param context  Subscription context
     * @param publish  MQTT publish message
     * @return Executor key
     */
    private String getExecutorKey(MqttMessageHandler.Ordering ordering, SubscriptionContext context, Mqtt5Publish publish) {
        switch (ordering) {
            case PER_TOPIC:
                return "topic-" + context.getTopic();
            case PER_CLIENT:
                return "client-" + context.getServerId() + "-" + publish.getTopic();
            default:
                return "default";
        }
    }

    /**
     * Gets or creates an ordered executor for the given key
     *
     * @param executorKey Executor identifier
     * @param annotation  Handler annotation configuration
     * @return ExecutorService instance, or null if not needed
     */
    private ExecutorService getOrderedExecutor(String executorKey, MqttMessageHandler annotation) {
        if (annotation.maxConcurrentMessages() <= 0) {
            return null; // No concurrent message limit, no need for ordered executor
        }

        return orderedExecutors.computeIfAbsent(executorKey, k -> {
            ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
            executor.setCorePoolSize(1);
            executor.setMaxPoolSize(1);
            executor.setQueueCapacity(annotation.maxConcurrentMessages());
            executor.setThreadNamePrefix("mqtt-ordered-" + executorKey + "-");
            executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
            executor.initialize();
            return executor.getThreadPoolExecutor();
        });
    }

    /**
     * Handles message with statistics tracking
     *
     * @param publish    MQTT publish message
     * @param context    Subscription context
     * @param statistics Statistics tracker
     */
    /**
     * Central dispatcher that honors the {@code async} flag using a real executor.
     * <p>
     * When async is enabled, the handler runs off the MQTT client thread on the
     * dedicated async executor. Otherwise it runs synchronously on the calling
     * thread (the documented behavior for async = false).
     *
     * @param publish    MQTT publish message
     * @param context    Subscription context
     * @param statistics Statistics tracker
     */
    private void dispatch(Mqtt5Publish publish, SubscriptionContext context, HandlerStatistics statistics) {
        if (context.getAnnotation().async()) {
            ExecutorService executor = getAsyncExecutor();
            if (executor != null) {
                executor.submit(() -> safeRun(publish, context, statistics));
                return;
            }
            // Executor unavailable (e.g. properties not bound) — fall back to
            // synchronous processing on the MQTT thread rather than dropping.
        }
        handleMessageCore(publish, context, statistics);
    }

    /**
     * Wraps core handling with exception logging for execution on a pool thread.
     *
     * @param publish    MQTT publish message
     * @param context    Subscription context
     * @param statistics Statistics tracker
     */
    private void safeRun(Mqtt5Publish publish, SubscriptionContext context, HandlerStatistics statistics) {
        try {
            handleMessageCore(publish, context, statistics);
        } catch (Exception e) {
            log.error("Error handling async message for topic {} on server {}: {}", publish.getTopic(), context.getServerId(), e.getMessage(), e);
        }
    }

    /**
     * Shuts down all ordered executors
     */
    public void shutdownOrderedExecutors() {
        for (Map.Entry<String, ExecutorService> entry : orderedExecutors.entrySet()) {
            try {
                entry.getValue().shutdown();
                if (!entry.getValue().awaitTermination(5, TimeUnit.SECONDS)) {
                    entry.getValue().shutdownNow();
                }
            } catch (InterruptedException e) {
                entry.getValue().shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        orderedExecutors.clear();
    }


    /**
     * Handles subscription result
     *
     * @param subAck Subscription acknowledgement
     * @param throwable Exception if subscription failed
     * @param context Subscription context
     */
    private void handleSubscriptionResult(com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck subAck, Throwable throwable, SubscriptionContext context) {

        if (throwable != null) {
            log.error("Failed to subscribe to topic {} on server {}: {}", context.getTopic(), context.getServerId(), throwable.getMessage(), throwable);
            context.transitionTo(SubscriptionState.FAILED);
            scheduleRetry(context);
        } else {
            log.info("Successfully subscribed to topic {} on server {} with reason codes: {}", context.getTopic(), context.getServerId(), subAck.getReasonCodes());
            context.transitionTo(SubscriptionState.SUBSCRIBED);
        }
    }

    // 修复3: 优化 scheduler 管理，避免内存泄漏
    private final ScheduledExecutorService retryScheduler = Executors.newScheduledThreadPool(2, r -> {
        Thread t = new Thread(r, "mqtt-subscribe-retry");
        t.setDaemon(true);
        return t;
    });

    /**
     * Core message handling logic
     *
     * @param publish    MQTT publish message
     * @param context    Subscription context
     * @param statistics Statistics tracker
     */
    private void handleMessageCore(Mqtt5Publish publish, SubscriptionContext context, HandlerStatistics statistics) {
        long startTime = System.nanoTime();

        try {
            // Get handler methods
            List<EnhancedHandlerMethod> handlerMethods = handlerMethodCache.get(context.getHandlerBean().getClass());
            if (handlerMethods == null || handlerMethods.isEmpty()) {
                log.warn("No handler methods found for bean: {} for topic: {}", context.getHandlerBean().getClass().getName(), context.getTopic());
                return;
            }

            // Sort by priority
            handlerMethods.sort(Comparator.comparingInt(EnhancedHandlerMethod::getPriority).reversed());

            // Invoke handler methods
            boolean success = false;
            for (EnhancedHandlerMethod handlerMethod : handlerMethods) {
                try {
                    Object result = handlerMethod.invoke(publish, context.getServerId(), statistics);
                    success = true;
                    log.debug("Successfully handled message for topic {} on server {} using method: {}", context.getTopic(), context.getServerId(), handlerMethod.getMethod().getName());
                    break; // Stop after first successful invocation
                } catch (Exception e) {
                    log.debug("Handler method {} failed for topic {}: {}", handlerMethod.getMethod().getName(), context.getTopic(), e.getMessage(), e);
                    // Continue to next handler method
                }
            }

            long processingTime = System.nanoTime() - startTime;

            // Update statistics
            if (success) {
                statistics.recordSuccess(processingTime);
            } else {
                statistics.recordFailure();
                log.warn("No suitable handler method found for topic {} on server {} for bean: {}", context.getTopic(), context.getServerId(), context.getHandlerBean().getClass().getName());
            }

        } catch (Exception e) {
            long processingTime = System.nanoTime() - startTime;
            statistics.recordFailure();
            log.error("Error handling MQTT message for topic {} on server {}: {}", context.getTopic(), context.getServerId(), e.getMessage(), e);
        }
    }


    /**
     * Gets handler statistics for a specific handler
     *
     * @param handlerClassName Handler class name
     * @return Map of handler statistics
     */
    public Map<String, Object> getHandlerStatistics(String handlerClassName) {
        Map<String, Object> stats = new HashMap<>();

        handlerStatistics.entrySet().stream().filter(entry -> entry.getKey().contains(handlerClassName)).findFirst().ifPresent(entry -> {
            HandlerStatistics statistics = entry.getValue();
            stats.put("totalMessages", statistics.getTotalMessages().get());
            stats.put("successCount", statistics.getSuccessCount().get());
            stats.put("failureCount", statistics.getFailureCount().get());
            stats.put("averageProcessingTimeMs", statistics.getAverageProcessingTimeMs());
            stats.put("throughput", statistics.getThroughput());
            stats.put("successRate", statistics.getSuccessRate());
            stats.put("maxProcessingTimeMs", statistics.getMaxProcessingTime().get() / 1_000_000.0);
            stats.put("minProcessingTimeMs", statistics.getMinProcessingTime().get() / 1_000_000.0);
            stats.put("lastProcessedAt", statistics.getLastProcessedAt().get());
        });

        return stats;
    }

    @Override
    public void destroy() {
        log.info("Cleaning up MQTT message handler processor resources...");

        // 关闭 retry scheduler
        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // 关闭 async executor
        if (asyncExecutor != null) {
            asyncExecutor.shutdown();
            try {
                if (!asyncExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    asyncExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                asyncExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            asyncExecutor = null;
        }

        // 关闭 ordered executors
        for (ExecutorService executor : orderedExecutors.values()) {
            executor.shutdown();
        }
        orderedExecutors.clear();

        // 清理其他资源
        subscriptionContexts.clear();
        handlerMethodCache.clear();
        processedMessages.clear();
        handlerStatistics.clear();
        handlerGroups.clear();

        log.info("MQTT message handler processor cleanup completed.");
    }

    /**
     * Daemon thread factory for MQTT executors so worker threads never block JVM exit.
     */
    private static class DaemonThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger(1);

        DaemonThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(@NotNull Runnable r) {
            Thread t = new Thread(r, prefix + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        }
    }

}
