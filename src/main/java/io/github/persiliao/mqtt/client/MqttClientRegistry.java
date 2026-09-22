package io.github.persiliao.mqtt.client;

import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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
public class MqttClientRegistry {

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
}
