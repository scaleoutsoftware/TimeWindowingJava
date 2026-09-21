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
package com.scaleoutsoftware.collections.timewindowing.samples;

import com.scaleoutsoftware.collections.timewindowing.*;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class WatermarkedSample {
    final static long ONE_MINUTE_MILLISECOND = 60000;
    static StringBuilder builder = new StringBuilder("");
    static String prefix = "";
    public static void main(String[] args) {
        // arbitrary max min heart rate
        int max = 185;
        int min = 40;
        Random r = new Random();

        long time = System.currentTimeMillis();
        long end = time+(ONE_MINUTE_MILLISECOND * 10);

        long start = time;
        long every = ONE_MINUTE_MILLISECOND;
        long duration = ONE_MINUTE_MILLISECOND * 2;
        int closedWindowCount = 0;
        double slidingAverage = 0;

        int numEvents = 100;
        int eventCount = 0;

        ArrayList<HeartRate> heartRates = new ArrayList<>();
        long expectedClosureEventTimestamp = start+(ONE_MINUTE_MILLISECOND*3);
        System.out.println("Repeatedly add events every 15 seconds into the collection starting at " + Timestamp.from(Instant.ofEpochMilli(start)).toString());
        System.out.println("The watermark generator will allow events to arrive that are 1-minute late. \n" +
                "The first window will close at: " + Timestamp.from(Instant.ofEpochMilli(expectedClosureEventTimestamp)).toString());

        // transform the ArrayList into an iterable collection of sliding windows where
        // each window is 2 minutes long, and a new window starts every minute
        WatermarkedSlidingWindowCollection<HeartRate> swc = new WatermarkedSlidingWindowCollection<>(heartRates, HeartRate::getTimestamp, start, duration, every, new LatenessToleranceWatermarkGenerator(ONE_MINUTE_MILLISECOND));
        for(; time < end; time+=(ONE_MINUTE_MILLISECOND/4)){
            HeartRate readingToAdd = new HeartRate(time, r.nextInt((max-min) + 1) + min);
            eventCount++;
            if(time == expectedClosureEventTimestamp) {
                System.out.println("Adding event (" + "HeartRate - " +
                        readingToAdd.getHeartRate() +
                        " Timestamp - " +
                        Timestamp.from(Instant.ofEpochMilli(readingToAdd.getTimestamp())).toString() + ") that will cause first window to close.");
                System.out.println("Event count BEFORE adding: " + eventCount + " Collection size BEFORE adding: " + heartRates.size());
            }

            List<TimeWindow<HeartRate>> closedWindows = swc.add(readingToAdd);
            if(time == expectedClosureEventTimestamp) {
                System.out.println("Event count AFTER adding: " + eventCount + " Collection size AFTER adding: " + heartRates.size());
                System.out.println("Only the events that are exclusively in the closed window are evicted.");
            }
            if(!closedWindows.isEmpty()) {
                for(TimeWindow<HeartRate> timeWindow : closedWindows) {
                    printWindowInfo(closedWindowCount++, timeWindow);
                    int hrSum = 0;
                    for(HeartRate hr : timeWindow) {
                        hrSum += hr.getHeartRate();
                        printWindowContentInfo(hr, swc.getNextWindowStartTimeMs());
                    }

                    slidingAverage += timeWindow.size() > 0 ? ((double) hrSum / timeWindow.size()) : 0;

                }
            }
        }
        System.out.println("Closed windows " + closedWindowCount + " source collection size: " + heartRates.size());
        System.out.println("Sliding average for closed windows: " + slidingAverage/closedWindowCount);
    }

    public static <V> void printWindowInfo(int count, TimeWindow<V> window) {
        builder.append(count > 0 ? "\t" : "");
        prefix = builder.toString();
        System.out.printf("%s%s%s%s%s%n",
                prefix,
                "Start Time - ",
                Timestamp.from(Instant.ofEpochMilli(window.getStartTimeMs())).toString(),
                " End Time - ",
                Timestamp.from(Instant.ofEpochMilli(window.getEndTimeMs())).toString());
    }

    public static void printWindowContentInfo(HeartRate hr, long nextWindowStartMs) {
        boolean evicted = hr.getTimestamp() < nextWindowStartMs;
        System.out.printf("%s%s%s%d%s%s%n",
                prefix,
                evicted ? "(EVICTED) " : "",
                "HeartRate - ",
                hr.getHeartRate(),
                " Timestamp - ",
                Timestamp.from(Instant.ofEpochMilli(hr.getTimestamp())).toString());
    }
}
