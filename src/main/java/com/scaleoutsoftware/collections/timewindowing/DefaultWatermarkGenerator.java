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

/**
 * The DefaultWatermarkGenerator does not change the default implementation of the TimeWindowing
 * eviction. The watermark is always set at the start time of the windowing collection, so only items
 * that arrive before the start time of the collection are evicted.
 */
public class DefaultWatermarkGenerator implements WatermarkGenerator {
    private final long _startTimeMs;

    /**
     * Instantiate the WatermarkGenerator with the start time of the windowing collection.
     * @param startTimeMs the start time of the windowing collection.
     */
    public DefaultWatermarkGenerator(long startTimeMs) {
        _startTimeMs = startTimeMs;
    }

    /**
     * Returns the start time of the windowing collection of which this {@link WatermarkGenerator} is associated
     * with.
     * @param lastEventTimestampMs *ignored*
     * @return the start time of the windowing collection of which this watermark generator is associated with.
     */
    @Override
    public long generateWatermark(long lastEventTimestampMs) {
        return _startTimeMs;
    }
}
