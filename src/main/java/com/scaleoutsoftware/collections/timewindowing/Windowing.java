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

import java.util.Iterator;
import java.util.List;

/**
 * Static functions that transform a List into an iterable collection of TimeWindows.
 */
public class Windowing {

    private Windowing() { }

    /**
     * Transforms a List into an iterable collection of session TimeWindows.
     * @param sourceCollection the source collection
     * @param timestampSelector the selector used to pull timestamps from objects
     * @param startTimeMs the start time to use when scanning the collection
     * @param timeoutDurationMs the minimum amount of time between session window ranges
	 * @param endTimeMs the end time to use when scanning the collection
     * @param <T> the type of objects in the source collection
     * @return an iterable collection of session windows
     */
    public static <T> Iterable<TimeWindow<T>> toSessionWindows(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long endTimeMs, long timeoutDurationMs) {
        if(sourceCollection.size() <= 0) {
            throw new NullPointerException("Underlying source collection has no items.");
        }
        return new SessionWindowIterable<T>(sourceCollection, timestampSelector, startTimeMs, endTimeMs, timeoutDurationMs);
    }

    /**
     * Transforms a List into an iterable collection of tumbling TimeWindows.
     * @param sourceCollection the source collection
     * @param timestampSelector the selector used to pull timestamps from objects
     * @param startTimeMs the start time to use when scanning the collection
	 * @param endTimeMs the end time to use when scanning the collection
     * @param windowDurationMs the length of time in each time window
     * @param <T> the type of objects in the source collection
     * @return an iterable collection of tumbling windows
     */
    public static <T> Iterable<TimeWindow<T>> toTumblingWindows(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long endTimeMs, long windowDurationMs) {
        return toSlidingWindows(sourceCollection, timestampSelector, startTimeMs, endTimeMs, windowDurationMs, windowDurationMs);
    }

    /**
     * Transforms a List into an iterable collection of sliding TimeWindows
     * @param sourceCollection the source collection
     * @param timestampSelector the selector used to pull timestamp from objects
     * @param startTimeMs the start time to use when scanning the collection
	 * @param endTimeMs the end time to use when scanning the collection
     * @param windowDurationMs the length of time in each time window
     * @param everyDurationMs the time between the starting point of each time window
     * @param <T> the type of objects in the source collection
     * @return an iterable collection of sliding windows
     */
    public static <T> Iterable<TimeWindow<T>> toSlidingWindows(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long endTimeMs, long windowDurationMs, long everyDurationMs) {
        if(sourceCollection.size() <= 0) {
            throw new NullPointerException("Underlying source collection has no items.");
        }

        return new SlidingWindowIterable<>(sourceCollection, timestampSelector, startTimeMs, endTimeMs, windowDurationMs, everyDurationMs);
    }

    static class SessionWindowIterable<T> implements Iterable<TimeWindow<T>> {
        List<T> _sourceCollection;
        TimestampSelector<T> _timestampSelector;
        long _startTimeMs;
        long _endTimeMs;
        long _timeoutMs;
        int _index;

        public SessionWindowIterable(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long endTimeMs, long timeoutMs) {
            _sourceCollection   = sourceCollection;
            _timestampSelector  = timestampSelector;
            _startTimeMs        = startTimeMs;
            _endTimeMs          = endTimeMs;
            _timeoutMs          = timeoutMs;
            _index              = 0;
        }

        @Override
        public Iterator<TimeWindow<T>> iterator() {
            return new Iterator<TimeWindow<T>>() {
                @Override
                public boolean hasNext() {
                    return _index < _sourceCollection.size();
                }

                @Override
                public TimeWindow<T> next() {
                    SessionTimeWindow<T> window = new SessionTimeWindow<T>(_timeoutMs);
                    _index = window.setItems(_sourceCollection, _index, _timestampSelector);
                    return window;
                }
            };
        }
    }


    static class SlidingWindowIterable<T> implements Iterable<TimeWindow<T>> {
        final List<T> _sourceCollection;
        final TimestampSelector<T> _timestampSelector;
        final long _endTimeMs;
        final long _windowDurationMs;
        final long _everyDurationMs;
        long _startTimeMs;
        int index;

        public SlidingWindowIterable(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long startTimeMs, long endTimeMs, long windowDurationMs, long everyDurationMs) {
            _sourceCollection   = sourceCollection;
            _timestampSelector  = timestampSelector;
            _startTimeMs        = startTimeMs;
            _endTimeMs          = endTimeMs;
            _windowDurationMs   = windowDurationMs;
            _everyDurationMs    = everyDurationMs;
            index               = 0;
        }

        @Override
        public Iterator<TimeWindow<T>> iterator() {
            return new Iterator<TimeWindow<T>>() {
                @Override
                public boolean hasNext() {
                    return _startTimeMs < _endTimeMs;
                }

                @Override
                public SlidingTimeWindow<T> next() {
                    SlidingTimeWindow<T> window = new SlidingTimeWindow<T>(_startTimeMs, _startTimeMs + _windowDurationMs);
                    index = window.setItems(_sourceCollection, index, _timestampSelector);
                    _startTimeMs = _startTimeMs + _everyDurationMs;
                    return window;
                }
            };
        }
    }
}
