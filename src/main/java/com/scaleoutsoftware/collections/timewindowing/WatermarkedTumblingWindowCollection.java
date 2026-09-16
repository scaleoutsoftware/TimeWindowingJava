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
 */
public class WatermarkedTumblingWindowCollection<T> implements Iterable<TimeWindow<T>> {
    private List<T> _sourceCollection;
    private TimestampSelector<T> _timestampSelector;
    private long _windowDurationMs;
    private long _watermarkMs;
    private long _nextWindowStartTimeMs;
    private WatermarkGenerator _watermarkGenerator;
    /**
     *
     * @param sourceCollection
     * @param timestampSelector
     * @param windowDurationMs
     * @param watermarkGenerator
     */
    public WatermarkedTumblingWindowCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long windowDurationMs, WatermarkGenerator watermarkGenerator) {
        init(sourceCollection, timestampSelector, startTimeMs, windowDurationMs, watermarkGenerator);
    }

    private void init(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long windowDurationMs, WatermarkGenerator watermarkGenerator) {
        if(sourceCollection == null) throw new IllegalArgumentException("Source collection is null.");
        if(timestampSelector == null) throw new IllegalArgumentException("timestampSelector is null.");
        if(windowDurationMs <= 0) throw new IllegalArgumentException("window duration is <= 0");
        if(watermarkGenerator == null) throw new IllegalArgumentException("watermark generator is null");
        _sourceCollection       = sourceCollection;
        _timestampSelector      = timestampSelector;
        _windowDurationMs       = windowDurationMs;
        _nextWindowStartTimeMs  = startTimeMs;
        _watermarkMs            = sourceCollection.isEmpty() ? Long.MIN_VALUE : timestampSelector.select(sourceCollection.get(sourceCollection.size()-1));
        _watermarkGenerator     = watermarkGenerator;
    }

    public List<TimeWindow<T>> add(T item) {
        boolean mutated = false;
        if (_sourceCollection.isEmpty()) {
            _sourceCollection.add(0, item);
            _watermarkMs = _watermarkGenerator.generateWatermark(_timestampSelector.select(item));
            mutated = true;
        } else {
            long currentEventTimestampMs = _timestampSelector.select(item);
            _watermarkMs = _watermarkGenerator.generateWatermark(currentEventTimestampMs);
            if(currentEventTimestampMs > _watermarkMs) {
                Utils.addTimeOrdered(_sourceCollection, _timestampSelector, item);
                mutated = true;
            }
        }
        if(mutated)
            return performEviction();
        else
            return Collections.emptyList();
    }

    private List<TimeWindow<T>> performEviction() {
        EvictionMetadata<T> ret = Utils.performWatermarkedWindowedEviction(
                _sourceCollection,
                _timestampSelector,
                _watermarkMs,
                _windowDurationMs,
                _windowDurationMs,
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
            return Windowing.toTumblingWindows(_sourceCollection, _timestampSelector, _nextWindowStartTimeMs, end, _windowDurationMs).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(_sourceCollection != null && !_sourceCollection.isEmpty()) {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            Windowing.toTumblingWindows(_sourceCollection, _timestampSelector, _nextWindowStartTimeMs, end, _windowDurationMs).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if(_sourceCollection == null || _sourceCollection.isEmpty()) {
            return Spliterators.emptySpliterator();
        } else {
            long end = _timestampSelector.select(_sourceCollection.get(_sourceCollection.size()-1)) + 1;
            return Windowing.toTumblingWindows(_sourceCollection, _timestampSelector, _nextWindowStartTimeMs, end, _windowDurationMs).spliterator();
        }
    }
}

