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
 * Eviction metadata stores metadata for the result of a eviction routine
 * @param <T> the type of the objects in the collection
 */
class EvictionMetadata<T> {
    private final long _nextWindowStartTimeMs;
    private final List<TimeWindow<T>> _closedWindows;

    EvictionMetadata(List<TimeWindow<T>> closedWindows) {
        _closedWindows = closedWindows;
        _nextWindowStartTimeMs = Long.MIN_VALUE;
    }

    EvictionMetadata(List<TimeWindow<T>> closedWindows, long nextWindowStartTimeMs) {
        _closedWindows = closedWindows;
        _nextWindowStartTimeMs = nextWindowStartTimeMs;
    }

    List<TimeWindow<T>> getClosedWindows() {
        return _closedWindows;
    }

    long getNextWindowStartTimeMs() {
        return _nextWindowStartTimeMs;
    }
}
