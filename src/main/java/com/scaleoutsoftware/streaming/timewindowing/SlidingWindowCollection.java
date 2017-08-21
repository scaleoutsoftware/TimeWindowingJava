/*
 Copyright (c) 2017 by ScaleOut Software, Inc.

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
package com.scaleoutsoftware.streaming.timewindowing;

import java.util.*;
import java.util.function.Consumer;

/**
 * The SlidingWindowCollection transforms a collection into an iterable collection of overlapping time windows. This
 * wrapper class can be used to manage the retention policy and add objects in chronological order to the underlying
 * source collection.
 */
public class SlidingWindowCollection<T> implements Iterable<TimeWindow<T>> {
    List<T> _source;
    private TimestampSelector<T> _selector;
    long _startTime;
    long _windowDuration;
    long _every;

    /**
     * Instantiates a new SlidingWindowCollection
     * @param source the underlying source collection
     * @param selector the interface used to select a timestamp from an item
     * @param windowDuration the duration of a time window
     * @param every the time between the starting point of each time window
     * @param startTime the first time an object can be in a time window -- items before the start time will be evicted from the source collection.
     */
    public SlidingWindowCollection(List<T> source, TimestampSelector<T> selector, long windowDuration, long every, long startTime) {
        init(source, selector, windowDuration, every, startTime);
    }

    private void init(List<T> source, TimestampSelector<T> selector, long windowDuration, long every, long startTime) {
        _source         = source;
        _selector       = selector;
        _windowDuration = windowDuration;
        _every          = every;
        _startTime      = startTime;

        performEviction();
    }

    /**
     * Adds an item to the underlying source collection in chronological order.
     * @param item the item to add
     */
    public void add(T item) {
        if(_source.size() == 0)
            _source.add(0, item);
        else
            Utils.addTimeOrdered(_source, _selector, item);

        performEviction();
    }

    private void performEviction() {
        Utils.performEviction(_source, _selector, _startTime);
    }
    @Override
    public Iterator<TimeWindow<T>> iterator() {
        if(_source == null || _source.size() == 0) {
            return Collections.emptyIterator();
        } else {
            long end = _selector.select(_source.get(_source.size()-1)) + 1;
            return Windowing.toSlidingWindows(_source, _selector, _startTime, end, _windowDuration, _every).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(_source != null && _source.size() != 0) {
            long end = _selector.select(_source.get(_source.size()-1)) + 1;
            Windowing.toSlidingWindows(_source, _selector, _startTime, end, _windowDuration, _every).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if (_source == null || _source.size() == 0) {
            return Spliterators.emptySpliterator();
        } else {
            long end = _selector.select(_source.get(_source.size() - 1)) + 1;
            return Windowing.toSlidingWindows(_source, _selector, _startTime, end, _windowDuration, _every).spliterator();
        }
    }

}
