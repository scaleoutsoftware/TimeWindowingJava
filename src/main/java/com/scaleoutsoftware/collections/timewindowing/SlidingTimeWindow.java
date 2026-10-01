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

    private long _startTimeMs;
    private long _endTimeMs;
    private List<T> _windowContents;

    SlidingTimeWindow(long startTimeMs, long endTimeMs) {
        _startTimeMs    = startTimeMs;
        _endTimeMs      = endTimeMs;
    }

    SlidingTimeWindow(long startTimeMs, long endTimeMs, List<T> windowContents) {
        _startTimeMs        = startTimeMs;
        _endTimeMs          = endTimeMs;
        _windowContents     = windowContents;
    }

    /**
     * Set the items in this window from the parameter source collection
     * @param sourceCollection the source collection to pull items from
     * @param startIndex the index to start scanning from
     * @param timestampSelector the selector used to pull timestamps from items
     * @return the last index touched
     */
    int setItems(List<T> sourceCollection, int startIndex, TimestampSelector<T> timestampSelector) {
        boolean foundItem = false;
        LinkedList<T> items = null;
        int i;
        // loop from the last index used, until we run out of items or the timestamp is greater than the end time
        // for this window
        for(i = startIndex; i < sourceCollection.size(); i++) {
            T item = sourceCollection.get(i);
            long timestamp = timestampSelector.select(item);
            if(timestamp < _startTimeMs)
                continue;

            if(timestamp >= _endTimeMs)
                break;

            if(!foundItem) {
                foundItem = true;
                startIndex = i;
                items = new LinkedList<T>();
            }

            items.add(item);
        }

        _windowContents = items;
        return startIndex;
    }

    @Override
    public int size() {
        if(_windowContents == null) {
            return 0;
        } else {
            return _windowContents.size();
        }
    }

    @Override
    public List<T> getWindowContents() {
        if(_windowContents == null) return Collections.emptyList();
        return _windowContents;
    }

    @Override
    public long getStartTimeMs() {
        return _startTimeMs;
    }

    @Override
    public long getEndTimeMs() {
        return _endTimeMs;
    }

    @Override
    public Iterator<T> iterator() {
        if(_windowContents == null)
            return Collections.emptyIterator();
        else {
            return _windowContents.iterator();
        }
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        if(_windowContents != null && _windowContents.size() > 0) {
            _windowContents.forEach(action);
        }
    }

    @Override
    public Spliterator<T> spliterator() {
        if(_windowContents == null) {
            return Spliterators.emptySpliterator();
        } else {
            return _windowContents.spliterator();
        }
    }
}
