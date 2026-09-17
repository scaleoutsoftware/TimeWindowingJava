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

import java.util.List;

/**
 * Used to transform a List into an iterable collection of {@link TimeWindow}.
 *
 * Items whose timestamp precede the collections start time are evicted from the collection.
 *
 * @param <T> the object type of the source collection.
 */
public abstract class WindowingCollection<T> implements Iterable<TimeWindow<T>> {
    /**
     * The source collection of items for a windowing collection.
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
     * Instantiate a new WindowingCollection.
     * @param sourceCollection the underlying source collection
     * @param timestampSelector the {@link TimestampSelector} is used to pull a timestamp from an item in the source
     *                          collection and subsequent insertions.
     * @param startTimeMs the first time an object can be in a time window -- items before the start time will
     *                    be evicted. The start time is also the start time of the first time window.
     */
    public WindowingCollection(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs) {
        if (sourceCollection == null) {throw new IllegalArgumentException("sourceCollection must not be null");}
        if (timestampSelector == null) {throw new IllegalArgumentException("timestampSelector must not be null");}
        if (startTimeMs < 0) {throw new IllegalArgumentException("startTimeMs must not be negative");}
        _sourceCollection   = sourceCollection;
        _timestampSelector  = timestampSelector;
        _startTimeMs        = startTimeMs;
    }

    /**
     * Adds an item to the underlying source collection in chronological order.
     * @param item the item to add.
     */
    public void add(T item) {
        if(_sourceCollection.isEmpty())
            _sourceCollection.add(0, item);
        else
            Utils.addTimeOrdered(_sourceCollection, _timestampSelector, item);

        performEviction();
    }

    abstract void performEviction();
}
