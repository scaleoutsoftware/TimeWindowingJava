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
 * The watermark generator is used to generate a watermark for the last seen event time.
 */
public interface WatermarkGenerator {
    /**
     * Generate (or return the existing) watermark for the last seen event timestamp.
     * @param lastEventTimestampMs the last seen event timestamp.
     * @return the existing or new watermark for this windowing collection.
     */
    public long generateWatermark(long lastEventTimestampMs);
}
