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

import java.util.Collections;
import java.util.List;

/**
 * Used to transform a List into an iterable collection of {@link TimeWindow}.
 *
 * Windows in a {@link WatermarkedWindowingCollection} can be closed if the watermark passes the inclusive end of
 * a window. The watermark also prevents items with timestamps that exceed the watermark from being added to the collection.
 *
 * Items that exclusively reside in closed windows are evicted from the collection.
 *
 * @param <T> the object type of the source collection.
 */
public abstract class WatermarkedWindowingCollection<T> implements Iterable<TimeWindow<T>> {
    /**
     * The source collection of items for the watermarked windowing collection.
     */
    protected List<T> _sourceCollection;
    /**
     * The timestamp selector is used to select a timestamp from an element in the source collection.
     */
    protected TimestampSelector<T> _timestampSelector;
    /**
     * The inclusive start time of first window of a windowing collection.
     */
    protected long _startTimeMs;
    /**
     * The watermark generator is used to generate a watermark for the windowing collection.
     */
    protected WatermarkGenerator _watermarkGenerator;
    /**
     * The current watermark of the windowing collection.
     */
    protected long _watermarkMs;

    /**
     * Instantiates a new SlidingWindowCollection
     * @param sourceCollection the underlying source collection
     * @param timestampSelector the {@link TimestampSelector} is used to pull a timestamp from an item in the source
     *                          collection and subsequent insertions.
     * @param startTimeMs the first time an object can be in a time window -- items before the start time will
     *                    be evicted. The start time is also the start time of the first time window.
     * @param watermarkGenerator the {@link WatermarkGenerator} is used to generate a watermark. Entries that arrive
     *                           before the watermark time are evicted. Windows whose inclusive end exceeds the watermark
     *                           are closed.
     */
    public WatermarkedWindowingCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, WatermarkGenerator watermarkGenerator) {
        if(sourceCollection == null) throw new IllegalArgumentException("Source collection is null.");
        if(timestampSelector == null) throw new IllegalArgumentException("timestampSelector is null.");
        if(watermarkGenerator == null) throw new IllegalArgumentException("watermark generator is null");
        _sourceCollection       = sourceCollection;
        _timestampSelector      = timestampSelector;
        _startTimeMs            = startTimeMs;
        _watermarkGenerator     = watermarkGenerator;
        _watermarkMs            = sourceCollection.isEmpty() ? Long.MIN_VALUE : timestampSelector.select(sourceCollection.get(sourceCollection.size()-1));
    }

    /**
     * Uses the watermark generator to generate a watermark from the parameter item. If the items timestamp exceeds the
     * watermark then the item is added to the source collection in time order. If the items timestamp equals or
     * preceeds the watermark, the add is ignored.
     *
     * The newly created watermark may cause windows in the collection to close. Windows whose inclusive end exceeds the
     * watermark are considered closed. Closed windows are returned. If no windows are closed, an empty list is returned.
     *
     * @param item the item to add.
     * @return returns a list of a closed windows.
     */
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

    abstract List<TimeWindow<T>> performEviction();

}
