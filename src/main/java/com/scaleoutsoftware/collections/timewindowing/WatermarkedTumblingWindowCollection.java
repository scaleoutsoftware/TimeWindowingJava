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
 * The TumblingWindowCollection transforms a collection into an iterable collection of sequential time windows. This
 * wrapper class can be used to manage the retention policy and add objects in chronological order to the underlying
 * source collection.
 *
 * The difference between {@link TumblingWindowCollection} and {@link WatermarkedTumblingWindowCollection} is that
 * windows in the {@link WatermarkedTumblingWindowCollection} can be closed if the watermark passes the inclusive end of
 * a window. The watermark also prevents items with timestamps that exceed the watermark from being added to the collection.
 *
 * @param <T> the object type of the source collection.
 */
public class WatermarkedTumblingWindowCollection<T> extends WatermarkedWindowingCollection<T> {
    private long windowDurationMs;
    private long nextWindowStartTimeMs;

    /**
     * Instantiates a new WatermarkedTumblingWindowCollection
     * @param sourceCollection the underlying source collection.
     * @param timestampSelector the {@link TimestampSelector} is used to pull a timestamp from an item in the source
     *                          collection and subsequent insertions.
     * @param startTimeMs the first time an object can be in a time window -- items before the start time will
     *                    be evicted. The start time is also the start time of the first time window.
     * @param windowDurationMs the window duration in milliseconds for each window.
     * @param watermarkGenerator the {@link WatermarkGenerator} is used to generate a watermark. Entries that arrive
     *                           before the watermark time are evicted. Windows whose inclusive end exceeds the watermark
     *                           are closed.
     */
    public WatermarkedTumblingWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long windowDurationMs, WatermarkGenerator watermarkGenerator) {
        super(sourceCollection, timestampSelector, startTimeMs, watermarkGenerator);
        init(windowDurationMs);
    }

    /**
     * Instantiates a new WatermarkedTumblingWindowCollection
     * @param sourceCollection the underlying source collection.
     * @param timestampSelector the {@link TimestampSelector} is used to pull a timestamp from an item in the source
     *                          collection and subsequent insertions.
     * @param nextWindowStartTimeMs the first time an object can be in a time window -- items before the start time will
     *                    be evicted. The start time is also the start time of the first time window.
     * @param windowDurationMs the window duration in milliseconds for each window.
     * @param watermarkGenerator the {@link WatermarkGenerator} is used to generate a watermark. Entries that arrive
     *                           before the watermark time are evicted. Windows whose inclusive end exceeds the watermark
     *                           are closed.
     * @param currentWatermarkMs the current watermark in milliseconds.
     */
    public WatermarkedTumblingWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long nextWindowStartTimeMs, long windowDurationMs, WatermarkGenerator watermarkGenerator, long currentWatermarkMs) {
        super(sourceCollection, timestampSelector, nextWindowStartTimeMs, watermarkGenerator);
        init(windowDurationMs);
        watermarkMs = currentWatermarkMs;
    }

    /**
     * Retrieves the windows duration in milliseconds.
     * @return the windows duration in milliseconds.
     */
    public long getWindowDurationMs() {
        return windowDurationMs;
    }

    /**
     * Retrieves the next window start time in milliseconds.
     * @return the next window start time in milliseconds.
     */
    public long getNextWindowStartTimeMs() {
        return nextWindowStartTimeMs;
    }

    private void init(long windowDurationMs) {
        if(windowDurationMs <= 0) throw new IllegalArgumentException("window duration is <= 0");
        this.windowDurationMs = windowDurationMs;
        nextWindowStartTimeMs = startTimeMs;
    }

    @Override
    List<TimeWindow<T>> performEviction() {
        EvictionMetadata<T> ret = Utils.performWatermarkedWindowedEviction(
                sourceCollection,
                timestampSelector,
                watermarkMs,
                windowDurationMs,
                windowDurationMs,
                nextWindowStartTimeMs);
        nextWindowStartTimeMs = ret.getNextWindowStartTimeMs();
        return ret.getClosedWindows();
    }

    @Override
    public Iterator<TimeWindow<T>> iterator() {
        if(sourceCollection == null || sourceCollection.isEmpty()) {
            return Collections.emptyIterator();
        } else {
            long end = timestampSelector.select(sourceCollection.get(sourceCollection.size()-1)) + 1;
            return Windowing.toTumblingWindows(sourceCollection, timestampSelector, nextWindowStartTimeMs, end, windowDurationMs).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(sourceCollection != null && !sourceCollection.isEmpty()) {
            long end = timestampSelector.select(sourceCollection.get(sourceCollection.size()-1)) + 1;
            Windowing.toTumblingWindows(sourceCollection, timestampSelector, nextWindowStartTimeMs, end, windowDurationMs).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if(sourceCollection == null || sourceCollection.isEmpty()) {
            return Spliterators.emptySpliterator();
        } else {
            long end = timestampSelector.select(sourceCollection.get(sourceCollection.size()-1)) + 1;
            return Windowing.toTumblingWindows(sourceCollection, timestampSelector, nextWindowStartTimeMs, end, windowDurationMs).spliterator();
        }
    }
}

