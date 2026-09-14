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
 * A sequence of items in a collection before the specified end time has been reached.
 */
class SlidingTimeWindow<T> implements TimeWindow<T> {

    private long _startTime;
    private long _endTime;
    private List<T> _items;

    SlidingTimeWindow(long startTime, long endTime) {
        _startTime  = startTime;
        _endTime    = endTime;
    }

    SlidingTimeWindow(long startTime, long endTime, List<T> items) {
        _startTime  = startTime;
        _endTime    = endTime;
        _items      = items;
    }

    /**
     * Set the items in this window from the parameter source collection
     * @param source the source collection to pull items from
     * @param startIndex the index to start scanning from
     * @param selector the selector used to pull timestamps from items
     * @return the last index touched
     */
    int setItems(List<T> source, int startIndex, TimestampSelector<T> selector) {
        boolean foundItem = false;
        LinkedList<T> items = null;
        int i;
        // loop from the last index used, until we run out of items or the timestamp is greater than the end time
        // for this window
        for(i = startIndex; i < source.size(); i++) {
            T item = source.get(i);
            long timestamp = selector.select(item);
            if(timestamp < _startTime)
                continue;

            if(timestamp >= _endTime)
                break;

            if(!foundItem) {
                foundItem = true;
                startIndex = i;
                items = new LinkedList<T>();
            }

            items.add(item);
        }

        _items = items;
        return startIndex;
    }

    @Override
    public int size() {
        if(_items == null) {
            return 0;
        } else {
            return _items.size();
        }
    }

    @Override
    public List<T> getItems() {
        if(_items == null) return Collections.emptyList();
        return _items;
    }

    @Override
    public long getStartTimeMs() {
        return _startTime;
    }

    @Override
    public long getEndTimeMs() {
        return _endTime;
    }

    @Override
    public Iterator<T> iterator() {
        if(_items == null)
            return Collections.emptyIterator();
        else {
            return _items.iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        if(_items != null && _items.size() > 0) {
            _items.forEach(action);
        }
    }

    @Override
    public Spliterator<T> spliterator() {
        if(_items == null) {
            return Spliterators.emptySpliterator();
        } else {
            return _items.spliterator();
        }
    }

    @Override
    public String toString() {
        return "SlidingTimeWindow{" +
                "_startTime=" + _startTime +
                ", _endTime=" + _endTime +
                ", size=" + (_items == null ? 0 : _items.size()) +
                '}';
    }
}
