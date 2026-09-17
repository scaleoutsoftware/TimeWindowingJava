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
 * The SlidingWindowCollection transforms a collection into an iterable collection of overlapping time windows. This
 * wrapper class can be used to manage the retention policy and add objects in chronological order to the underlying
 * source collection.
 *
 * The difference between {@link SlidingWindowCollection} and {@link WatermarkedSlidingWindowCollection} is that
 * windows in the {@link WatermarkedSlidingWindowCollection} can be closed if the watermark passes the inclusive end of
 * a window. The watermark also prevents items with timestamps that exceed the watermark from being added to the collection.
 *
 * @param <T> the object type of the source collection.
 */
public class WatermarkedSlidingWindowCollection<T> extends WatermarkedWindowingCollection<T> {
    private long _windowDurationMs;
    private long _everyMs;
    private long _nextWindowStartTimeMs;

    /**
     * Instantiates a new WatermarkedSlidingWindowCollection.
     * @param sourceCollection the underlying source collection
     * @param timestampSelector the {@link TimestampSelector} is used to pull a timestamp from an item in the source
     *                          collection and subsequent insertions.
     * @param startTimeMs the first time an object can be in a time window -- items before the start time will
     *                    be evicted. The start time is also the start time of the first time window.
     * @param windowDurationMs the duration of a time window
     * @param everyMs the time between the starting point of each time window
     * @param watermarkGenerator the {@link WatermarkGenerator} is used to generate a watermark. Entries that arrive
     *                           before the watermark time are evicted. Windows whose inclusive end exceeds the watermark
     *                           are closed.
     */
    public WatermarkedSlidingWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long windowDurationMs, long everyMs, WatermarkGenerator watermarkGenerator) {
        super(sourceCollection, timestampSelector, startTimeMs, watermarkGenerator);
        init(windowDurationMs, everyMs);
    }

    private void init(long windowDurationMs, long everyMs) {
        if(windowDurationMs <=0) throw new IllegalArgumentException("window duration is <= 0 in param");
        if(everyMs <= 0) throw new IllegalArgumentException("everyMs is <= 0 in param");
        _windowDurationMs       = windowDurationMs;
        _everyMs                = everyMs;
        _nextWindowStartTimeMs  = _startTimeMs;
    }

    @Override
    List<TimeWindow<T>> performEviction() {
        EvictionMetadata<T> ret = Utils.performWatermarkedWindowedEviction(
                _sourceCollection,
                _timestampSelector,
                _watermarkMs,
                _windowDurationMs,
                _everyMs,
                _nextWindowStartTimeMs);
        _nextWindowStartTimeMs = ret.getNextWindowStartTimeMs();
        return ret.getClosedWindows();
    }

    @Override
    public Iterator<TimeWindow<T>> iterator() {
        if(_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Collections.emptyIterator();
        } else {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            return Windowing.toSlidingWindows(_sourceCollection, _timestampSelector, _nextWindowStartTimeMs, end, _windowDurationMs, _everyMs).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(_sourceCollection != null && !_sourceCollection.isEmpty()) {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            Windowing.toSlidingWindows(_sourceCollection, _timestampSelector, _nextWindowStartTimeMs, end, _windowDurationMs, _everyMs).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if (_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Spliterators.emptySpliterator();
        } else {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            return Windowing.toSlidingWindows(_sourceCollection, _timestampSelector, _nextWindowStartTimeMs, end, _windowDurationMs, _everyMs).spliterator();
        }
    }

}
