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
 * The LatenessToleranceWatermarkGenerator generates watermarks with a constant lateness tolerance.
 */
public class LatenessToleranceWatermarkGenerator implements WatermarkGenerator {
    private final long _latenessToleranceMs;
    private long _currentWaterMarkMs = Long.MIN_VALUE;

    /**
     * Constructs a new LatenessToleranceWatermarkGenerator with a constant lateness tolerance.
     * @param latenessToleranceMs the lateness tolerance in milliseconds.
     */
    public LatenessToleranceWatermarkGenerator(long latenessToleranceMs) {
        _latenessToleranceMs = latenessToleranceMs;
    }

    /**
     * Generates (or returns the existing) watermark based on the last seen event timestamp.
     * @param lastEventTimestampMs the last seen event timestamp.
     * @return the new or existing watermark for a windowing collection.
     */
    @Override
    public long generateWatermark(long lastEventTimestampMs) {
        long watermarkOption = lastEventTimestampMs - _latenessToleranceMs;

        if (watermarkOption > _currentWaterMarkMs) {
            _currentWaterMarkMs = watermarkOption;
        }

        return _currentWaterMarkMs;
    }

    /**
     * Returns the current watermark in milliseconds.
     * @return the current watermark in milliseconds.
     */
    public long currentWaterMarkMs() {
        return _currentWaterMarkMs;
    }
}
