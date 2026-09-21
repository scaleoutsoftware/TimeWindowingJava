/*
 Copyright (c) 2026 by ScaleOut Software, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/
package com.scaleoutsoftware.collections.timewindowing;

import java.util.*;
import java.util.function.Consumer;

/**
 * The WatermarkedSessionWindowCollection transforms a List into an iterable collection of session windows. This wrapper
 * class can be used to manage the retention policy of the source collection as well as to insert new objects in
 * chronological order.
 *
 * The difference between {@link SessionWindowCollection} and {@link WatermarkedSessionWindowCollection} is that
 * windows in the {@link WatermarkedSessionWindowCollection} can be closed if the watermark passes the inclusive end of
 * a window.
 *
 * @param <T> the object type of the source collection.
 */
public class WatermarkedSessionWindowCollection<T> extends WatermarkedWindowingCollection<T> {
    private long timeoutMs;
    private long nextWindowStartTimeMs;

    /**
     * Instantiates a new SessionWindowCollection
     * @param sourceCollection the underlying source collection.
     * @param timestampSelector the {@link TimestampSelector} is used to pull a timestamp from an item in the source
     *                          collection and subsequent insertions.
     * @param startTimeMs the first time an object can be in a time window -- items before the start time will
     *                    be evicted. The start time is also the start time of the first time window.
     * @param timeoutMs the minimum amount of time between session window ranges.
     * @param watermarkGenerator the {@link WatermarkGenerator} is used to generate a watermark. Entries that arrive
     *                           before the watermark time are evicted. Windows whose inclusive end exceeds the watermark
     *                           are closed.
     */
    public WatermarkedSessionWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long timeoutMs, WatermarkGenerator watermarkGenerator) {
        super(sourceCollection, timestampSelector, startTimeMs, watermarkGenerator);
        init(timeoutMs);
    }

    /**
     * Instantiates a new SessionWindowCollection
     * @param sourceCollection the underlying source collection.
     * @param timestampSelector the {@link TimestampSelector} is used to pull a timestamp from an item in the source
     *                          collection and subsequent insertions.
     * @param nextWindowStartTimeMs the first time an object can be in a time window -- items before the start time will
     *                    be evicted. The start time is also the start time of the first time window.
     * @param timeoutMs the minimum amount of time between session window ranges.
     * @param watermarkGenerator the {@link WatermarkGenerator} is used to generate a watermark. Entries that arrive
     *                           before the watermark time are evicted. Windows whose inclusive end exceeds the watermark
     *                           are closed.
     * @param currentWatermarkMs the current watermark in milliseconds.
     */
    public WatermarkedSessionWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long nextWindowStartTimeMs, long timeoutMs, WatermarkGenerator watermarkGenerator, long currentWatermarkMs) {
        super(sourceCollection, timestampSelector, nextWindowStartTimeMs, watermarkGenerator);
        init(timeoutMs);
        watermarkMs = currentWatermarkMs;
    }

    /**
     * Retrieve the configured session window timeout in milliseconds.
     * @return the timeout in milliseconds.
     */
    public long getTimeoutMs() {
        return timeoutMs;
    }

    private void init(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    List<TimeWindow<T>> performEviction() {
        EvictionMetadata<T> ret = Utils.performSessionWindowEviction(sourceCollection, timestampSelector, watermarkMs, timeoutMs);
        return ret.getClosedWindows();
    }

    @Override
    public Iterator<TimeWindow<T>> iterator() {
        if(sourceCollection == null || sourceCollection.isEmpty()) {
            return Collections.emptyIterator();
        } else {
            long end = timestampSelector.select(sourceCollection.get(sourceCollection.size()-1)) + 1;
            return Windowing.toSessionWindows(sourceCollection, timestampSelector, nextWindowStartTimeMs, end, timeoutMs).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(sourceCollection != null && !sourceCollection.isEmpty()) {
            long end = timestampSelector.select(sourceCollection.get(sourceCollection.size()-1)) + 1;
            Windowing.toSessionWindows(sourceCollection, timestampSelector, nextWindowStartTimeMs, end, timeoutMs).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if(sourceCollection == null || sourceCollection.isEmpty()) {
            return Spliterators.emptySpliterator();
        } else {
            long end = timestampSelector.select(sourceCollection.get(sourceCollection.size()-1)) + 1;
            return Windowing.toSessionWindows(sourceCollection, timestampSelector, nextWindowStartTimeMs, end, timeoutMs).spliterator();
        }
    }
}
