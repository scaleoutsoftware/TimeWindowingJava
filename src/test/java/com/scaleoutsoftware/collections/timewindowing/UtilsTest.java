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

        EvictionMetadata<TestObject> ret = Utils.performWatermarkedWindowedEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                windowSizeMs,
                everyMs,
                startTimeMs);

        List<TimeWindow<TestObject>> closedWindows = ret.getClosedWindows();

        assertEquals("Expected exactly one window to close", 1, closedWindows.size());

        TimeWindow<TestObject> closedWindow = closedWindows.get(0);

        // Assert window bounds [0, 100)
        assertEquals(0, closedWindow.getStartTimeMs());
        assertEquals(100, closedWindow.getEndTimeMs());
        List<TestObject> items = closedWindow.getWindowContents();
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

        EvictionMetadata<TestObject> ret = Utils.performWatermarkedWindowedEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                windowSizeMs,
                everyMs,
                startTimeMs);

        List<TimeWindow<TestObject>> closedWindows = ret.getClosedWindows();

        assertEquals("Expected exactly one window to close", 1, closedWindows.size());

        TimeWindow<TestObject> closedWindow = closedWindows.get(0);
        assertEquals(0, closedWindow.getStartTimeMs());
        assertEquals(50, closedWindow.getEndTimeMs());
        List<TestObject> items = closedWindow.getWindowContents();
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

        EvictionMetadata<TestObject> ret = Utils.performWatermarkedWindowedEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                windowSizeMs,
                everyMs,
                startTimeMs);

        List<TimeWindow<TestObject>> closedWindows = ret.getClosedWindows();

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
            List<TestObject> items = window.getWindowContents();
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

    /*
     * Session windowing tests:
     */

    @Test
    public void testPerformSessionWindowEvictionNoClosedWindow() {
        List<TestObject> sourceCollection = new ArrayList<TestObject>();

        sourceCollection.add(new TestObject(0));
        sourceCollection.add(new TestObject(1));
        sourceCollection.add(new TestObject(2));
        sourceCollection.add(new TestObject(3));

        long watermarkMs = 20;
        long timeoutMs = 4;

        List<TimeWindow<TestObject>> closedWindows = new ArrayList<TimeWindow<TestObject>>();

        EvictionMetadata<TestObject> ret = Utils.performSessionWindowEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                timeoutMs);

        assertEquals("No session should have closed",0, closedWindows.size());

        assertEquals(4, sourceCollection.size());

        for (int i = 0; i < 4; i++) {
            assertEquals(i, sourceCollection.get(i).getTimestamp());
        }
    }

    @Test
    public void testPerformSessionWindowEvictionOneClosedWindow() {
        List<TestObject> sourceCollection = new ArrayList<TestObject>();

        long[] sessionEventTimestamps = {
                0, 1, 2, 3,
                9, 10, 11
        };

        for (long timestamp : sessionEventTimestamps) {
            sourceCollection.add(new TestObject(timestamp));
        }

        long watermarkMs = 10;
        long timeoutMs = 4;

        /*
         * Sessions:
         *
         * [0, 1, 2, 3]
         * [9, 10, 11]
         *
         * Gap:
         *
         * 9 - 3 = 6 > 4 (timeoutMs)
         *
         * First session timeout boundary:
         *
         * 3 + 4 = 7
         *
         * 10 (watermark) > 3, therefore the session can close.
         */
        EvictionMetadata<TestObject> ret = Utils.performSessionWindowEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                timeoutMs);

        List<TimeWindow<TestObject>> closedWindows = ret.getClosedWindows();

        assertEquals(1, closedWindows.size());

        TimeWindow<TestObject> window = closedWindows.get(0);

        assertEquals(0, window.getStartTimeMs());
        assertEquals(3, window.getEndTimeMs());

        assertEquals(4, window.getWindowContents().size());

        for (int i = 0; i < 4; i++) {
            assertEquals(i, window.getWindowContents().get(i).getTimestamp());
        }

        /*
         * First session was finalized and evicted.
         *
         * Remaining open session:
         *
         * [9, 10, 11]
         */
        assertEquals(3, sourceCollection.size());

        assertEquals(9, sourceCollection.get(0).getTimestamp());
        assertEquals(10, sourceCollection.get(1).getTimestamp());
        assertEquals(11, sourceCollection.get(2).getTimestamp());
    }

    @Test
    public void testPerformSessionWindowEvictionMultipleClosedWindows() {
        List<TestObject> sourceCollection = new ArrayList<TestObject>();

        long[] sessionEventTimestamps = {
                0, 1, 2, 3,
                9, 10, 11,
                18, 19, 20
        };

        for (long timestamp : sessionEventTimestamps) {
            sourceCollection.add(new TestObject(timestamp));
        }

        long watermarkMs = 20;
        long timeoutMs = 4;

        /*
         * Sessions:
         *
         * [0, 1, 2, 3]
         * [9, 10, 11]
         * [18, 19, 20]
         *
         * Break #1:
         *
         * 9 - 3 = 6 > 4 (timeoutMs)
         *
         * First session timeout boundary:
         *
         * 3 + 4 = 7 <= 20 (watermarkMs)
         *
         *
         * Break #2:
         *
         * 18 - 11 = 7 > 4 (timeoutMs)
         *
         * Second session timeout boundary:
         *
         * 11 + 4 = 15 <= 20 (watermarkMs)
         *
         *
         * No element follows 20, so the third session remains open.
         */
        EvictionMetadata<TestObject> ret = Utils.performSessionWindowEviction(
                sourceCollection,
                TestObject::getTimestamp,
                watermarkMs,
                timeoutMs);

        List<TimeWindow<TestObject>> closedWindows = ret.getClosedWindows();

        assertEquals(2, closedWindows.size());

        long[][] expectedWindows = {
                {0, 3},
                {9, 11}
        };

        long[][] expectedItems = {
                {0, 1, 2, 3},
                {9, 10, 11}
        };

        for (int i = 0; i < closedWindows.size(); i++) {
            TimeWindow<TestObject> window = closedWindows.get(i);

            assertEquals(expectedWindows[i][0], window.getStartTimeMs());

            assertEquals(expectedWindows[i][1], window.getEndTimeMs());

            assertEquals(expectedItems[i].length, window.getWindowContents().size());

            for (int j = 0; j < expectedItems[i].length; j++) {
                assertEquals(expectedItems[i][j], window.getWindowContents().get(j).getTimestamp());
            }
        }

        /*
         * Remaining active session:
         *
         * [18, 19, 20]
         */
        assertEquals(3, sourceCollection.size());

        assertEquals(18, sourceCollection.get(0).getTimestamp());
        assertEquals(19, sourceCollection.get(1).getTimestamp());
        assertEquals(20, sourceCollection.get(2).getTimestamp());
    }
}
