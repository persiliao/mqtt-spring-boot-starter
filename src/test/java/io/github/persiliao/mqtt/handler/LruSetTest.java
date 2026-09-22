package io.github.persiliao.mqtt.handler;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the bounded {@link LruSet} used for duplicate detection.
 */
class LruSetTest {

    @Test
    void firstOccurrenceIsNotADuplicate() {
        LruSet set = new LruSet(4);
        assertThat(set.addIfAbsent(1L)).isTrue();
        assertThat(set.addIfAbsent(1L)).isFalse();
        assertThat(set.size()).isEqualTo(1);
    }

    @Test
    void evictsOldestEntryWhenBoundIsReached() {
        LruSet set = new LruSet(2);
        assertThat(set.addIfAbsent(1L)).isTrue();
        assertThat(set.addIfAbsent(2L)).isTrue();
        assertThat(set.addIfAbsent(3L)).isTrue();

        assertThat(set.size()).isEqualTo(2);
        // 1 was evicted when 3 was added
        assertThat(set.addIfAbsent(1L)).isTrue();
        // 3 is still present
        assertThat(set.addIfAbsent(3L)).isFalse();
    }

    @Test
    void accessRefreshesEvictionOrder() {
        LruSet set = new LruSet(2);
        set.addIfAbsent(1L);
        set.addIfAbsent(2L);
        // touching 1 makes 2 the eviction candidate
        assertThat(set.addIfAbsent(1L)).isFalse();
        set.addIfAbsent(3L);

        assertThat(set.addIfAbsent(1L)).isFalse(); // still present
        assertThat(set.addIfAbsent(2L)).isTrue();  // was evicted
    }
}
