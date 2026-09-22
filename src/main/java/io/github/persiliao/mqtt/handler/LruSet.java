package io.github.persiliao.mqtt.handler;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Bounded set of message fingerprints used for duplicate detection.
 *
 * <p>Implemented as an access-ordered {@link LinkedHashMap} (via
 * {@link Collections#newSetFromMap}), so the least-recently-used entry is
 * evicted once the bound is exceeded. The memory footprint therefore stays
 * constant regardless of traffic volume.
 *
 * @since 3.0.0
 */
final class LruSet {

    private final Set<Long> storage;

    /**
     * Creates a new bounded set.
     *
     * @param maxSize the maximum number of retained entries
     */
    LruSet(int maxSize) {
        Map<Long, Boolean> map = new LinkedHashMap<Long, Boolean>(128, 1.0f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
                return size() > maxSize;
            }
        };
        this.storage = Collections.synchronizedSet(Collections.newSetFromMap(map));
    }

    /**
     * Adds an element, evicting the least-recently-used one when the bound is
     * reached.
     *
     * @param element the fingerprint
     * @return {@code true} if the element was <em>not</em> already present
     *         (first occurrence), {@code false} if it is a duplicate
     */
    boolean addIfAbsent(long element) {
        synchronized (storage) {
            return storage.add(element);
        }
    }

    /**
     * @return the number of currently retained entries
     */
    int size() {
        synchronized (storage) {
            return storage.size();
        }
    }
}
