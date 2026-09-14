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

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UtilsTest {

    @Test
    public void testPerformWatermarkedWindowedSourceEmpty() {
        List<TestObject> sourceCollection = new ArrayList<TestObject>();

        for (long timestamp = 1; timestamp <= 50; timestamp++) {
            sourceCollection.add(new TestObject(timestamp));
        }

        long startTimeMs = 0;
        long endTimeMs = 100;

        long watermarkMs = endTimeMs;
        long windowSizeMs = endTimeMs - startTimeMs;
        long everyMs = 100;

        // add closed windows to a collection to verify events fire
        List<TimeWindow<TestObject>> closedWindows = new ArrayList<TimeWindow<TestObject>>();
        WindowClosedHandler<TestObject> windowClosedHandler = closedWindows::add;

        Utils.performWatermarkedWindowedEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                windowSizeMs,
                everyMs,
                startTimeMs,
                windowClosedHandler);

        assertEquals("Expected exactly one window to close", 1, closedWindows.size());

        TimeWindow<TestObject> closedWindow = closedWindows.get(0);

        // Assert window bounds [0, 100)
        assertEquals(0, closedWindow.getStartTimeMs());
        assertEquals(100, closedWindow.getEndTimeMs());
        List<TestObject> items = closedWindow.getItems();
        assertEquals(50, items.size());
        for (int i = 0; i < 50; i++) {
            assertEquals(i + 1, items.get(i).getTimestamp());
        }

        assertTrue("Expected all source elements to be evicted", sourceCollection.isEmpty());
    }

    @Test
    public void testPerformWatermarkedWindowedSourceSplit() {
        List<TestObject> sourceCollection = new ArrayList<TestObject>();

        for (long timestamp = 1; timestamp <= 100; timestamp++) {
            sourceCollection.add(new TestObject(timestamp));
        }

        long startTimeMs = 0;
        long endTimeMs = 100;
        long watermarkMs = 50;
        long windowSizeMs = 50;
        long everyMs = 50;

        List<TimeWindow<TestObject>> closedWindows = new ArrayList<TimeWindow<TestObject>>();

        WindowClosedHandler<TestObject> windowClosedHandler = closedWindows::add;

        Utils.performWatermarkedWindowedEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                windowSizeMs,
                everyMs,
                startTimeMs,
                windowClosedHandler);

        assertEquals("Expected exactly one window to close", 1, closedWindows.size());

        TimeWindow<TestObject> closedWindow = closedWindows.get(0);
        assertEquals(0, closedWindow.getStartTimeMs());
        assertEquals(50, closedWindow.getEndTimeMs());
        List<TestObject> items = closedWindow.getItems();
        assertEquals(50, items.size());
        for (int i = 0; i < 50; i++) {
            assertEquals(i + 1, items.get(i).getTimestamp());
        }
        assertEquals(51, sourceCollection.size());
        assertEquals(50, sourceCollection.get(0).getTimestamp());
        assertEquals(100, sourceCollection.get(50).getTimestamp());
    }

    @Test
    public void testPerformWatermarkedWindowedEvictionOverLapping() {
        List<TestObject> sourceCollection = new ArrayList<TestObject>();

        for (long timestamp = 1; timestamp <= 100; timestamp++) {
            sourceCollection.add(new TestObject(timestamp));
        }

        long startTimeMs = 0;
        long endTimeMs = 100;

        long watermarkMs = 90;
        long windowSizeMs = 10;
        long everyMs = 5;
        List<TimeWindow<TestObject>> closedWindows = new ArrayList<TimeWindow<TestObject>>();

        WindowClosedHandler<TestObject> windowClosedHandler = closedWindows::add;

        Utils.performWatermarkedWindowedEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                windowSizeMs,
                everyMs,
                startTimeMs,
                windowClosedHandler);

        // Closed windows:
        //
        // [0,  10]
        // [5,  15]
        // [10, 20]
        // ...
        // [70, 80]
        // [75, 85]
        // [80, 90]
        //
        // The next window is:
        //
        // [85, 95]
        assertEquals("Expected 17 windows to close", 17, closedWindows.size());

        // loop through closed windows...
        for (int i = 0; i < closedWindows.size(); i++) {
            TimeWindow<TestObject> window = closedWindows.get(i);

            long expectedStartTimeMs = i * 5L;
            long expectedEndTimeMs = expectedStartTimeMs + 10L;

            assertEquals(expectedStartTimeMs, window.getStartTimeMs());

            assertEquals(expectedEndTimeMs, window.getEndTimeMs());
            for (TestObject item : window) {
                long itemTimestamp = item.getTimestamp();
                assertTrue(itemTimestamp>=expectedStartTimeMs && itemTimestamp<=expectedEndTimeMs);
            }
            List<TestObject> items = window.getItems();
            // first window has only 10 elements (starts at 1)
            if(i == 0) {
                assertEquals(10, items.size());
            } else {
                // all other windows are "full" [10, 20]
                assertEquals(11, items.size());
            }

        }

        /*
         * The next still-open window is:
         *
         * [85, 95]
         *
         * Source list should retain elements 85-100
         */
        assertEquals("Expected timestamps 85-100 to remain", 16, sourceCollection.size());
        /*
         * Verify elements of the entire retained source collection.
         */
        for (int i = 0; i < sourceCollection.size(); i++) {
            assertEquals(85 + i, sourceCollection.get(i).getTimestamp());
        }
    }
}
