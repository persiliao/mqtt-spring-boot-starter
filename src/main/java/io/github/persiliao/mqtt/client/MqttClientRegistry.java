package io.github.persiliao.mqtt.client;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Registry of the MQTT client instances managed by the starter, together with
 * their current connection state.
 *
 * <p>Clients are registered by the {@link MqttClientFactory} at creation time
 * and report state changes through the client lifecycle listeners. A
 * {@link ClientEventListener} (the {@link MqttSubscriptionManager}) is
 * notified whenever a client becomes connected or disconnected so that
 * subscriptions can be (re)established.
 *
 * @since 3.0.0
 */
public class MqttClientRegistry implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MqttClientRegistry.class);

    /**
     * Time granted to each client to send its DISCONNECT packet during
     * shutdown before the attempt is abandoned.
     */
    private static final long DISCONNECT_TIMEOUT_SECONDS = 5L;

    /**
     * Callback for client connection state changes.
     */
    public interface ClientEventListener {

        /**
         * Invoked when a client (re)connected.
         *
         * @param serverId the server id
         * @param client   the client
         */
        void onConnected(String serverId, Mqtt5AsyncClient client);

        /**
         * Invoked when a client disconnected.
         *
         * @param serverId the server id
         * @param client   the client
         */
        void onDisconnected(String serverId, Mqtt5AsyncClient client);
    }

    private final Map<String, Mqtt5AsyncClient> clients = new ConcurrentHashMap<>();
    private final Map<String, Boolean> connectionState = new ConcurrentHashMap<>();
    private volatile ClientEventListener eventListener;

    /**
     * Sets the connection state listener.
     *
     * @param eventListener the listener (may be {@code null} to remove)
     */
    public void setClientEventListener(ClientEventListener eventListener) {
        this.eventListener = eventListener;
    }

    /**
     * Registers a client. Its state is considered disconnected until the
     * first connected event fires.
     *
     * @param serverId the server id
     * @param client   the client instance
     */
    public void register(String serverId, Mqtt5AsyncClient client) {
        clients.put(serverId, client);
        connectionState.put(serverId, false);
    }

    /**
     * Marks a client as connected and notifies the listener.
     *
     * @param serverId the server id
     */
    public void markConnected(String serverId) {
        Mqtt5AsyncClient client = clients.get(serverId);
        if (client == null) {
            return;
        }
        connectionState.put(serverId, true);
        ClientEventListener listener = eventListener;
        if (listener != null) {
            listener.onConnected(serverId, client);
        }
    }

    /**
     * Marks a client as disconnected and notifies the listener.
     *
     * @param serverId the server id
     */
    public void markDisconnected(String serverId) {
        Mqtt5AsyncClient client = clients.get(serverId);
        if (client == null) {
            return;
        }
        connectionState.put(serverId, false);
        ClientEventListener listener = eventListener;
        if (listener != null) {
            listener.onDisconnected(serverId, client);
        }
    }

    /**
     * Looks up a client by server id.
     *
     * @param serverId the server id
     * @return the client, if any
     */
    public Optional<Mqtt5AsyncClient> find(String serverId) {
        return Optional.ofNullable(clients.get(serverId));
    }

    /**
     * @return an unmodifiable view of all registered clients
     */
    public Map<String, Mqtt5AsyncClient> allClients() {
        return Collections.unmodifiableMap(clients);
    }

    /**
     * @param serverId the server id
     * @return {@code true} when the client is currently connected
     */
    public boolean isConnected(String serverId) {
        return Boolean.TRUE.equals(connectionState.get(serverId));
    }

    /**
     * @return a snapshot of the connection state of all registered servers
     */
    public Map<String, Boolean> connectionStatus() {
        return Map.copyOf(connectionState);
    }

    /**
     * Sends a DISCONNECT packet for every connected client so that the broker
     * does not keep the session until the keep-alive/expiry timeout elapses.
     *
     * <p>Failures are logged and never propagated: shutdown must not fail
     * because a broker is unreachable.
     */
    public void disconnectAll() {
        for (Map.Entry<String, Mqtt5AsyncClient> entry : clients.entrySet()) {
            String serverId = entry.getKey();
            if (!Boolean.TRUE.equals(connectionState.get(serverId))) {
                continue;
            }
            try {
                entry.getValue().disconnect().get(DISCONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                log.debug("Disconnected MQTT client of server '{}'", serverId);
            } catch (Exception e) {
                log.debug("Could not gracefully disconnect MQTT client of server '{}': {}",
                        serverId, e.toString());
            }
        }
    }

    /**
     * Gracefully disconnects all connected clients and clears the registry.
     */
    @Override
    public void destroy() {
        disconnectAll();
        clients.clear();
        connectionState.clear();
        eventListener = null;
    }
}
