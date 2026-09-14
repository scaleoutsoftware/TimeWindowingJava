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
 * The SessionWindowCollection transforms a List into an iterable collection of session windows. This wrapper
 * class can be used to manage the retention policy of the source collection as well as to insert new objects in
 * chronological order.
 */
public class SessionWindowCollection<T> implements Iterable<TimeWindow<T>> {
    private List<T> _sourceCollection;
    private TimestampSelector<T> _timestampSelector;
    private long _startTimeMs;
    private long _timeoutMs;
    private long _watermarkMs;
    private long _nextWindowStartTimeMs;
    private WatermarkGenerator _watermarkGenerator;
    private WindowClosedHandler<T> _windowClosedHandler;

    /**
     * Instantiates a new SessionWindowCollection
     * @param sourceCollection the underlying source.
     * @param timestampSelector the selector used to pull a timestamp from an item in the source collection and subsequent insertions
     * @param startTimeMs the first time an object can be in a time window -- items before the start time will be evicted from the source collection.
     * @param timeoutMs the minimum amount of time between session window ranges
     */
    public SessionWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long timeoutMs) {
        init(sourceCollection, timestampSelector, startTimeMs, timeoutMs, new DefaultWatermarkGenerator(startTimeMs));
    }

    /**
     * Instantiates a new SessionWindowCollection
     * @param sourceCollection the underlying source collection.
     * @param timestampSelector the selector used to pull a timestamp from an item in the source collection for subsequent insertions.
     * @param startTimeMs the first time an object can be in a time window -- items before the start time will be evicted from the source collection.
     * @param timeoutMs the minimum amount of time between session window ranges
     * @param watermarkGenerator used to generate a watermark. Entries that arrive before the watermark time are evicted.
     */
    public SessionWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long timeoutMs, WatermarkGenerator watermarkGenerator) {
        init(sourceCollection, timestampSelector, startTimeMs, timeoutMs, watermarkGenerator);
    }

    private void init(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long timeoutMs, WatermarkGenerator watermarkGenerator) {
        _sourceCollection       = sourceCollection;
        _timestampSelector      = timestampSelector;
        _startTimeMs            = startTimeMs;
        _timeoutMs              = timeoutMs;
        _nextWindowStartTimeMs  = sourceCollection.isEmpty() ? 0 : timestampSelector.select(sourceCollection.get(0));
        _watermarkMs            = startTimeMs;
        _watermarkGenerator     = watermarkGenerator;

        performEviction();
    }

    public void registerWindowClosedHandler(WindowClosedHandler<T> windowClosedHandler) {
        if(windowClosedHandler == null) throw new IllegalArgumentException("Unexpected null window closed handler in param.");
        _windowClosedHandler = windowClosedHandler;
    }

    public long getWatermark() {
        return _watermarkMs;
    }

    /**
     * Adds an item to the source collection in time ordered fashion.
     * @param item the item to add
     */
    public void add(T item) {
        boolean mutated = false;
        if (_sourceCollection.isEmpty()) {
            mutated = true; // it's possible the first item we add is immediately evicted due to watermark.
            _sourceCollection.add(0, item);
            _watermarkMs = _watermarkGenerator.generateWatermark(_timestampSelector.select(item));
        } else {
            long currentEventTimestampMs = _timestampSelector.select(item);
            _watermarkMs = _watermarkGenerator.generateWatermark(currentEventTimestampMs);
            if(currentEventTimestampMs > _watermarkMs) {
                Utils.addTimeOrdered(_sourceCollection, _timestampSelector, item);
                mutated = true;
            }
        }

        if(mutated)
            performEviction();
    }

    private void performEviction() {
        Utils.performSessionWindowEviction(_sourceCollection, _timestampSelector, _watermarkMs, _timeoutMs, _windowClosedHandler);
    }

    @Override
    public Iterator<TimeWindow<T>> iterator() {
        if(_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Collections.emptyIterator();
        } else {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            return Windowing.toSessionWindows(_sourceCollection, _timestampSelector, _startTimeMs, end, _timeoutMs).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(_sourceCollection != null && !_sourceCollection.isEmpty()) {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            Windowing.toSessionWindows(_sourceCollection, _timestampSelector, _startTimeMs, end, _timeoutMs).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if(_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Spliterators.emptySpliterator();
        } else {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            return Windowing.toSessionWindows(_sourceCollection, _timestampSelector, _startTimeMs, end, _timeoutMs).spliterator();
        }
    }
}
