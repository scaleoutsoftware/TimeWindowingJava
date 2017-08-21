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
 * The SessionWindowCollection transforms a List into an iterable collection of session windows. This wrapper
 * class can be used to manage the retention policy of the source collection as well as to insert new objects in
 * chronological order.
 */
public class SessionWindowCollection<T> implements Iterable<TimeWindow<T>> {
    List<T> _source;
    TimestampSelector<T> _selector;
    long _startTime;
    long _timeout;

    /**
     * Instantiates a new SessionWindowCollection
     * @param source the underlying source.
     * @param selector the selector used to pull a timestamp from an item in the source collection and subsequent insertions
     * @param startTime the first time an object can be in a time window -- items before the start time will be evicted from the source collection.
     * @param timeout the minimum amount of time between session window ranges
     */
    public SessionWindowCollection(List<T> source, TimestampSelector<T> selector, long startTime, long timeout) {
        init(source, selector, startTime, timeout);
    }
    private void init(List<T> source, TimestampSelector<T> selector, long startTime, long timeout) {
        _source     = source;
        _selector   = selector;
        _startTime  = startTime;
        _timeout    = timeout;

        performEviction();
    }

    /**
     * Adds an item to the source collection in time ordered fashion.
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
            return Windowing.toSessionWindows(_source, _selector, _startTime, end, _timeout).iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super TimeWindow<T>> action) {
        if(_source != null && _source.size() > 0) {
            long end = _selector.select(_source.get(_source.size()-1)) + 1;
            Windowing.toSessionWindows(_source, _selector, _startTime, end, _timeout).forEach(action);
        }
    }

    @Override
    public Spliterator<TimeWindow<T>> spliterator() {
        if(_source == null || _source.size() == 0) {
            return Spliterators.emptySpliterator();
        } else {
            long end = _selector.select(_source.get(_source.size()-1)) + 1;
            return Windowing.toSessionWindows(_source, _selector, _startTime, end, _timeout).spliterator();
        }
    }
}
