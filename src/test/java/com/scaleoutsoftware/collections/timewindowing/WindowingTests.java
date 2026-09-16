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

import com.scaleoutsoftware.collections.timewindowing.samples.Sample;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WindowingTests {

    @Test
    public void testSessionWindowTimeout() {
        long timeout = 100;
        long startTimeMs = 1;
        ArrayList<TestObject> test = new ArrayList<TestObject>();

        WatermarkedSessionWindowCollection<TestObject> swc = new WatermarkedSessionWindowCollection<>(test,
                TestObject::getTimestamp,
                startTimeMs,
                timeout,
                new DefaultWatermarkGenerator(startTimeMs));

        swc.add(new TestObject(10));
        swc.add(new TestObject(15));
        swc.add(new TestObject(20));

        swc.add(new TestObject(125));
        swc.add(new TestObject(130));
        swc.add(new TestObject(135));

        swc.add(new TestObject(245));
        swc.add(new TestObject(250));
        swc.add(new TestObject(255));

        assertEquals(9, test.size());
        int windowCount = 0;
        for(TimeWindow<TestObject> window : swc) {
            windowCount++;
            int windowItemCount = 0;
            for(TestObject t : window) {
                assertTrue(t.getTimestamp() >= window.getStartTimeMs() && t.getTimestamp() <= window.getEndTimeMs());
                windowItemCount++;
            }
            assertEquals(3, windowItemCount);
        }
        assertEquals(3, windowCount);
    }

    @Test
    public void testSessionWindowTimeoutWithWatermark() {
        long timeout = 100;
        long startTimeMs = 1;
        ArrayList<TestObject> test = new ArrayList<TestObject>();

        WatermarkedSessionWindowCollection<TestObject> swc = new WatermarkedSessionWindowCollection<>(test,
                TestObject::getTimestamp,
                startTimeMs,
                timeout,
                new LatenessToleranceWatermarkGenerator(10));

        List<TimeWindow<TestObject>> closedWindows = new ArrayList<TimeWindow<TestObject>>();

        closedWindows.addAll(swc.add(new TestObject(10)));
        closedWindows.addAll(swc.add(new TestObject(15)));
        closedWindows.addAll(swc.add(new TestObject(20)));

        closedWindows.addAll(swc.add(new TestObject(125)));
        closedWindows.addAll(swc.add(new TestObject(130)));
        closedWindows.addAll(swc.add(new TestObject(135)));

        closedWindows.addAll(swc.add(new TestObject(245)));
        closedWindows.addAll(swc.add(new TestObject(250)));
        closedWindows.addAll(swc.add(new TestObject(255)));

        assertEquals(3, test.size());
        assertEquals(2, closedWindows.size());
        assertEquals(3, closedWindows.get(0).size());
        assertEquals(3, closedWindows.get(1).size());
        int windowCount = 0;
        for(TimeWindow<TestObject> window : swc) {
            windowCount++;
            int windowItemCount = 0;
            for(TestObject t : window) {
                assertTrue(t.getTimestamp() >= window.getStartTimeMs() && t.getTimestamp() <= window.getEndTimeMs());
                windowItemCount++;
            }
            assertEquals(3, windowItemCount);
        }
        assertEquals(1, windowCount);
    }

    @Test
    public void testSlidingWindowDuration() {
        int numElements = 100;
        long start = 0;
        long duration = 20;
        long every = 10;
        ArrayList<TestObject> test = new ArrayList<TestObject>();

        WatermarkedSlidingWindowCollection<TestObject> swc = new WatermarkedSlidingWindowCollection<>(test,
                TestObject::getTimestamp,
                start,
                duration,
                every,
                new DefaultWatermarkGenerator(start));

        for(int i = 0; i < numElements; i++) {
            swc.add(new TestObject(i));
        }
        assertEquals(100, test.size());
        int windowCount = 0;
        for(TimeWindow<TestObject> window : swc) {
            assertEquals(duration, (window.getEndTimeMs()-window.getStartTimeMs()));
            for(TestObject t : window) {
                assertTrue(t.getTimestamp() >= window.getStartTimeMs() && t.getTimestamp() < window.getEndTimeMs());
            }
            windowCount++;
        }
        assertEquals(10, windowCount);
    }

    @Test
    public void testIncrementalAddWatermarking() {
        List<TestObject> source = new ArrayList<>(100);
        long everyMs = 5;
        long durationMs = 10;
        long startTimeMs = 0;
        WatermarkedSlidingWindowCollection<TestObject> collection = new WatermarkedSlidingWindowCollection<TestObject>(source, TestObject::getTimestamp, startTimeMs, durationMs, everyMs, new LatenessToleranceWatermarkGenerator(5));
        List<TimeWindow<TestObject>> closedWindows = new ArrayList<TimeWindow<TestObject>>();


        for(int i = 0; i < 20; i++) {
            closedWindows.addAll(collection.add(new TestObject(i)));
        }
        assertEquals(1, closedWindows.size());

        TimeWindow<TestObject> window = closedWindows.get(0);

        assertEquals(0, window.getStartTimeMs());
        assertEquals(10, window.getEndTimeMs());

        assertTrue(!window.getItems().isEmpty() &&window.getItems().size() <= 11);

        for (TestObject item : window) {
            long itemTimestamp = item.getTimestamp();
            assertTrue(itemTimestamp >= window.getStartTimeMs() && itemTimestamp <= window.getEndTimeMs());
        }

        assertEquals(15, source.size());
        assertEquals(5, source.get(0).getTimestamp());
        assertEquals(19, source.get(source.size() - 1).getTimestamp());

    }

    @Test
    public void testIncrementalAddWatermarkingLarge() {
        List<TestObject> source = new ArrayList<>(100);
        long everyMs = 5;
        long durationMs = 10;
        long startTimeMs = 0;
        WatermarkedSlidingWindowCollection<TestObject> collection = new WatermarkedSlidingWindowCollection<TestObject>(source, TestObject::getTimestamp, startTimeMs, durationMs, everyMs, new LatenessToleranceWatermarkGenerator(5));
        List<TimeWindow<TestObject>> closedWindows = new ArrayList<TimeWindow<TestObject>>();

        for(int i = 0; i < 100; i++) {
            closedWindows.addAll(collection.add(new TestObject(i)));
        }

        assertEquals(17, closedWindows.size());

        for (int i = 0; i < closedWindows.size(); i++) {
            TimeWindow<TestObject> window = closedWindows.get(i);

            long expectedStartTimeMs = i * everyMs;
            long expectedEndTimeMs = expectedStartTimeMs + durationMs;

            assertEquals("Unexpected window start at index " + i, expectedStartTimeMs, window.getStartTimeMs());

            assertEquals("Unexpected window end at index " + i, expectedEndTimeMs, window.getEndTimeMs());

            List<TestObject> items = window.getItems();

            assertEquals("Unexpected item count for window at index " + i, 11, items.size());

            for (TestObject item : window) {
                long timestamp = item.getTimestamp();
                assertTrue("Unexpected timestamp in window " + i, timestamp >= expectedStartTimeMs && timestamp <= expectedEndTimeMs);
            }
        }
    }

    @Test
    public void testWindowClosedEmpty() {
        List<TestObject> source = new ArrayList<>(100);
        long everyMs = 10;
        long durationMs = 10;
        long startTimeMs = 0;
        WatermarkedSlidingWindowCollection<TestObject> collection = new WatermarkedSlidingWindowCollection<TestObject>(source, TestObject::getTimestamp, startTimeMs, durationMs, everyMs, new LatenessToleranceWatermarkGenerator(5));
        List<TimeWindow<TestObject>> closedWindows = new ArrayList<TimeWindow<TestObject>>();

        for(int i = 11; i < 21; i++) {
            closedWindows.addAll(collection.add(new TestObject(i)));
        }

        assertEquals(1, closedWindows.size()); // close one window, should be empty

        for (int i = 0; i < closedWindows.size(); i++) {
            TimeWindow<TestObject> window = closedWindows.get(i);

            long expectedStartTimeMs = i * everyMs;
            long expectedEndTimeMs = expectedStartTimeMs + durationMs;

            assertEquals("Unexpected window start at index " + i, expectedStartTimeMs, window.getStartTimeMs());

            assertEquals("Unexpected window end at index " + i, expectedEndTimeMs, window.getEndTimeMs());

            assertEquals(0, window.size());
        }
    }


    /*
     * Non-watermarking tests
     */

    @Test
    public void testEviction() {
        int numElements = 100;
        long start = 1;
        long duration = 10;
        long middle = 50;
        ArrayList<TestObject> collection = new ArrayList<TestObject>();

        TumblingWindowCollection<TestObject> swc = new TumblingWindowCollection<>(collection,
                TestObject::getTimestamp,
                10,
                start);


        for(int i = 1; i <= numElements; i++) {
            TestObject object = new TestObject(i);
            swc.add(object);
        }
        assertEquals(numElements, collection.size());

        TumblingWindowCollection<TestObject> twcEvictHalf = new TumblingWindowCollection<>(collection,
                TestObject::getTimestamp,
                duration,
                middle+1); // make window 41-51 close

        assertEquals(50, collection.size());

        TumblingWindowCollection<TestObject> twcEvict = new TumblingWindowCollection<>(collection,
                TestObject::getTimestamp,
                duration,
                101);

        assertEquals(0, collection.size());
    }

    @Test
    public void testTumblingWindowDuration() {
        int numElements = 100;
        long start = 1;
        long duration = 20;
        ArrayList<TestObject> test = new ArrayList<TestObject>();

        TumblingWindowCollection<TestObject> swc = new TumblingWindowCollection<>(test,
                TestObject::getTimestamp,
                duration,
                start);
        for(int i = 1; i <= numElements; i++) {
            swc.add(new TestObject(i));
        }
        assertEquals(100, test.size());
        int windowCount = 0;
        for(TimeWindow<TestObject> window : swc) {
            windowCount++;
            assertTrue((window.getEndTimeMs()-window.getStartTimeMs()) <= duration);
            for(TestObject t : window) {
                assertTrue(t.getTimestamp() >= window.getStartTimeMs() && t.getTimestamp() < window.getEndTimeMs());
            }
        }
        assertEquals(5, windowCount);
    }

    @Test
    public void testUtilsAdd() {
        int numElements = 100;
        long start = 1;
        long duration = 20;
        ArrayList<TestObject> tumblingSource = new ArrayList<TestObject>();

        TumblingWindowCollection<TestObject> twc = new TumblingWindowCollection<>(tumblingSource,
                TestObject::getTimestamp,
                duration,
                start);
        for(int i = 0; i <= numElements; i++) {
            twc.add(new TestObject(i));
        }

        assertEquals(numElements, tumblingSource.size());
    }

    @Test
    public void TestUtilsAddToFront() {
        ArrayList<TestObject> list = new ArrayList<>(25);
        long start = 0;
        long every = 10;
        long duration = 20;
        SlidingWindowCollection<TestObject> swc = new SlidingWindowCollection<TestObject>(list,
                TestObject::getTimestamp,
                duration,
                every,
                start);
        swc.add(new TestObject(2));
        swc.add(new TestObject(1));
        assertEquals(2, list.size());
        assertEquals(list.get(0).getTimestamp(), 1);
    }
}
