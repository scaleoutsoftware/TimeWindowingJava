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
 * The sequence of items in a collection before a predetermined timeout between items has been reached.
 */
class SessionTimeWindow<T> implements TimeWindow<T> {
    long _startTimeMs;
    long _endTimeMs;
    long _timeoutMs;
    List<T> _windowContents;
    int _size;


    SessionTimeWindow(long timeoutMs) {
        _timeoutMs      = timeoutMs;
        _startTimeMs    = 0;
        _endTimeMs      = 0;
    }

    SessionTimeWindow(long timeoutMs, long startTimeMs, long endTimeMs, List<T> windowContents) {
        _timeoutMs          = timeoutMs;
        _startTimeMs        = startTimeMs;
        _endTimeMs          = endTimeMs;
        _windowContents     = windowContents;
    }

    /**
     * Set the items used in this SessionWindow.
     * @param sourceCollection the source collection list to iterate over.
     * @param startIndex the index in the source collection list to start at
     * @param timestampSelector the {@link TimestampSelector} used to pull a timestamp out of an object
     * @return the last touched index.
     */
    int setItems(List<T> sourceCollection, int startIndex, TimestampSelector<T> timestampSelector) {
        LinkedList<T> list = new LinkedList<T>();
        long prev = 0, cur = 0;
        boolean first = true;
        int index;
        // start at the last used index, loop until the timeout between items is reached
        for(index = startIndex; index < sourceCollection.size(); index++) {
            T itemCur = sourceCollection.get(index);
            cur = timestampSelector.select(itemCur);
            if(first) {
                prev = cur;
                first = false;
                // set the start time of this window
                _startTimeMs = prev;
                list.add(itemCur);
                continue;
            }

            if(cur-prev > _timeoutMs) {
                // set the end time of this window
                _endTimeMs = prev;
                break;
            } else {
                prev = cur;
                list.add(itemCur);
                _size++;
            }
            _endTimeMs = cur;
        }

        _windowContents = list;
        return index;
    }

    @Override
    public List<T> getWindowContents() {
        if(_windowContents == null) return Collections.emptyList();
        return _windowContents;
    }

    @Override
    public int size() {
        if(_windowContents != null)
            return _windowContents.size();
        else
            return 0;
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
        if(_windowContents != null && !_windowContents.isEmpty()) {
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

    @Override
    public long getStartTimeMs() {
        return _startTimeMs;
    }

    @Override
    public long getEndTimeMs() {
        return _endTimeMs;
    }
}
