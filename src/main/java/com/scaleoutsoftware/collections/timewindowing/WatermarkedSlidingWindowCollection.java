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
 */
public class WatermarkedSlidingWindowCollection<T> implements Iterable<TimeWindow<T>> {
    private List<T> _sourceCollection;
    private TimestampSelector<T> _timestampSelector;
    private long _windowDurationMs;
    private long _everyMs;
    private long _watermarkMs;
    private long _nextWindowStartTimeMs;
    private WatermarkGenerator _watermarkGenerator;
    private WindowClosedHandler<T> _windowClosedHandler;

    /**
     * Instantiates a new SlidingWindowCollection
     * @param sourceCollection the underlying source collection
     * @param timestampSelector the interface used to select a timestamp from an item
     * @param windowDurationMs the duration of a time window
     * @param everyMs the time between the starting point of each time window
     * @param watermarkGenerator used to generate a watermark. Entries that arrive before the watermark time are evicted.
     */
    public WatermarkedSlidingWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long windowDurationMs, long everyMs, WatermarkGenerator watermarkGenerator) {
        init(sourceCollection, timestampSelector, windowDurationMs, everyMs, watermarkGenerator);
    }

    private void init(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long windowDurationMs, long everyMs, WatermarkGenerator watermarkGenerator) {
        if(sourceCollection == null) throw new IllegalArgumentException("Source collection is null in param.");
        if(timestampSelector == null) throw new IllegalArgumentException("timestampSelector is null in param.");
        if(windowDurationMs <=0) throw new IllegalArgumentException("window duration is <= 0 in param");
        if(everyMs <= 0) throw new IllegalArgumentException("everyMs is <= 0 in param");
        if(watermarkGenerator == null) throw new IllegalArgumentException("watermark generator is null in param");
        _sourceCollection       = sourceCollection;
        _timestampSelector      = timestampSelector;
        _windowDurationMs       = windowDurationMs;
        _everyMs                = everyMs;
        _nextWindowStartTimeMs  = sourceCollection.isEmpty() ? 0 : timestampSelector.select(sourceCollection.get(0));
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
     * Adds an item to the underlying source collection in chronological order.
     * @param item the item to add
     * @return returns closed windows
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
        _nextWindowStartTimeMs = Utils.performWatermarkedWindowedEviction(
                _sourceCollection,
                _timestampSelector,
                _watermarkMs,
                _windowDurationMs,
                _everyMs,
                _nextWindowStartTimeMs,
                _windowClosedHandler);
    }

    @Override
    public Iterator<TimeWindow<T>> iterator() {
        if(_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Collections.emptyIterator();
        } else {
            long start = _timestampSelector.select(_sourceCollection.get(0));
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            return Windowing.toSlidingWindows(_sourceCollection, _timestampSelector, start, end, _windowDurationMs, _everyMs).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(_sourceCollection != null && !_sourceCollection.isEmpty()) {
            long start = _timestampSelector.select(_sourceCollection.get(0));
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            Windowing.toSlidingWindows(_sourceCollection, _timestampSelector, start, end, _windowDurationMs, _everyMs).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if (_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Spliterators.emptySpliterator();
        } else {
            long start = _timestampSelector.select(_sourceCollection.get(0));
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size() - 1)) + 1;
            return Windowing.toSlidingWindows(_sourceCollection, _timestampSelector, start, end, _windowDurationMs, _everyMs).spliterator();
        }
    }

}
