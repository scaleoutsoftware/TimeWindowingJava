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
public class TumblingWindowCollection<T> extends WindowingCollection<T> {
    private long _windowDurationMs;

    /**
     * Instantiates a new tumbling window collection.
     * @param sourceCollection the underlying source collection.
     * @param timestampSelector the {@link TimestampSelector} is used to pull a timestamp from an item in the source
     *                          collection and subsequent insertions.
     * @param startTimeMs the first time an object can be in a time window -- items before the start time will
     *                    be evicted. The start time is also the start time of the first time window.
     * @param windowDurationMs the window duration in milliseconds for each window.

     */
    public TumblingWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long windowDurationMs) {
        super(sourceCollection, timestampSelector, startTimeMs);
        init(windowDurationMs);
    }

    private void init(long windowDurationMs) {
        _windowDurationMs   = windowDurationMs;

        performEviction();
    }

    void performEviction() {
        Utils.performEviction(_sourceCollection, _timestampSelector, _startTimeMs);
    }

    @Override
    public Iterator<TimeWindow<T>> iterator() {
        if(_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Collections.emptyIterator();
        } else {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            return Windowing.toTumblingWindows(_sourceCollection, _timestampSelector, _startTimeMs, end, _windowDurationMs).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(_sourceCollection != null && !_sourceCollection.isEmpty()) {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            Windowing.toTumblingWindows(_sourceCollection, _timestampSelector, _startTimeMs, end, _windowDurationMs).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if(_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Spliterators.emptySpliterator();
        } else {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            return Windowing.toTumblingWindows(_sourceCollection, _timestampSelector, _startTimeMs, end, _windowDurationMs).spliterator();
        }
    }
}

