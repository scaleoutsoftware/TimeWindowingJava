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

import java.util.Iterator;
import java.util.List;

/**
 * Static functions that transform a List into an iterable collection of TimeWindows.
 */
public class Windowing {

    /**
     * Transforms a List into an iterable collection of session TimeWindows.
     * @param source the source collection
     * @param selector the selector used to pull timestamps from objects
     * @param start the start time to use when scanning the collection
     * @param timeout the minimum amount of time between session window ranges
	 * @param end the end time to use when scanning the collection
     * @param <T> the type of objects in the source collection
     * @return an iterable collection of session windows
     */
    public static <T> Iterable<TimeWindow<T>> toSessionWindows(List<T> source, TimestampSelector<T> selector, long start, long end, long timeout) {
        if(source.size() <= 0) {
            throw new NullPointerException("Underlying source collection has no items.");
        }
        return new SessionWindowIterable<T>(source, selector, start, end, timeout);
    }

    /**
     * Transforms a List into an iterable collection of tumbling TimeWindows.
     * @param source the source collection
     * @param selector the selector used to pull timestamps from objects
     * @param start the start time to use when scanning the collection
	 * @param end the end time to use when scanning the collection
     * @param duration the length of time in each time window
     * @param <T> the type of objects in the source collection
     * @return an iterable collection of tumbling windows
     */
    public static <T> Iterable<TimeWindow<T>> toTumblingWindows(List<T> source, TimestampSelector<T> selector, long start, long end, long duration) {
        return toSlidingWindows(source, selector, start, end, duration, duration);
    }

    /**
     * Transforms a List into an iterable collection of sliding TimeWindows
     * @param source the source collection
     * @param selector the selector used to pull timestamp from objects
     * @param start the start time to use when scanning the collection
	 * @param end the end time to use when scanning the collection
     * @param duration the length of time in each time window
     * @param every the time between the starting point of each time window
     * @param <T> the type of objects in the source collection
     * @return an iterable collection of sliding windows
     */
    public static <T> Iterable<TimeWindow<T>> toSlidingWindows(List<T> source, TimestampSelector<T> selector, long start, long end, long duration, long every) {
        if(source.size() <= 0) {
            throw new NullPointerException("Underlying source collection has no items.");
        }

        return new SlidingWindowIterable<>(source, selector, start, end, duration, every);
    }

    static class SessionWindowIterable<T> implements Iterable<TimeWindow<T>> {
        List<T> _source;
        TimestampSelector<T> _selector;
        long _startTime;
        long _endTime;
        long _timeout;
        int _index;

        public SessionWindowIterable(List<T> source, TimestampSelector<T> selector, long start, long end, long timeout) {
            _source     = source;
            _selector   = selector;
            _startTime  = start;
            _endTime    = end;
            _timeout    = timeout;
            _index      = 0;
        }

        @Override
        public Iterator<TimeWindow<T>> iterator() {
            return new Iterator<TimeWindow<T>>() {
                @Override
                public boolean hasNext() {
                    return _index < _source.size();
                }

                @Override
                public TimeWindow<T> next() {
                    SessionTimeWindow<T> window = new SessionTimeWindow<T>(_timeout);
                    _index = window.setItems(_source, _index, _selector);
                    return window;
                }
            };
        }
    }


    static class SlidingWindowIterable<T> implements Iterable<TimeWindow<T>> {
        final List<T> _source;
        final TimestampSelector<T> _selector;
        long _start;
        final long _end;
        final long _duration;
        final long _every;
        int index;

        public SlidingWindowIterable(List<T> source, TimestampSelector<T> selector, long start, long end, long duration, long every) {
            _source     = source;
            _selector   = selector;
            _start      = start;
            _end        = end;
            _duration   = duration;
            _every      = every;
            index       = 0;
        }

        @Override
        public Iterator<TimeWindow<T>> iterator() {
            return new Iterator<TimeWindow<T>>() {
                @Override
                public boolean hasNext() {
                    return _start < _end;
                }

                @Override
                public SlidingTimeWindow<T> next() {
                    long dur = _duration;
                    if((_start + dur) > _end) {
                        dur = _end - _start;
                    }
                    SlidingTimeWindow<T> window = new SlidingTimeWindow<T>(_start, _start + dur);
                    index = window.setItems(_source, index, _selector);
                    _start = _start + _every;
                    return window;
                }
            };
        }
    }
}
