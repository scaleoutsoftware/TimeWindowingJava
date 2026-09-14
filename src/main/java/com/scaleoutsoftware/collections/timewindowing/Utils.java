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

import java.util.ArrayList;
import java.util.List;

/**
 * Helper functions used within the library.
 */
public class Utils {

    /**
     * Adds an item to the parameter source collection in chronological order.
     * @param source the source collection
     * @param selector the timestamp selector used to pull a timestamp from an item
     * @param toAdd the item to add
     * @param <T> the type of items in the source collection
     */
    public static <T> void addTimeOrdered(List<T> source, TimestampSelector<T> selector, T toAdd) {
        int index = source.size() - 1;
        long timeToAdd = selector.select(toAdd);
        long last = selector.select(source.get(index));

        if(timeToAdd < last) {
            while(index > 0) {
                if(timeToAdd < last) {
                    index--;
                    last = selector.select(source.get(index));
                } else {
                    index++;
                    break;
                }
            }
            source.add(index, toAdd);
        } else {
            source.add(++index, toAdd);
        }
    }

    /**
     * Removes items from the parameter source collection that have timestamps before the start time
     * @param sourceCollection the source collection
     * @param timestampSelector the timestamp selector used to pull a timestamp from an item
     * @param removeBeforeMs the timestamp for eviction for the collection -- items before this timestamp are removed.
     * @param <T> the type of the items in the source collection
     */
    public static <T> void performEviction(List<T> sourceCollection, TimestampSelector<T> timestampSelector, long removeBeforeMs) {
        if(sourceCollection != null) {
            int from = 0, to = 0;
            boolean clear = false;
            while (to < sourceCollection.size()) {
                if (timestampSelector.select(sourceCollection.get(to)) < removeBeforeMs) {
                    to++;
                    clear = true;
                } else {
                    break;
                }
            }

            if (clear)
                sourceCollection.subList(from, to).clear();
        }
    }


    /**
     * Removes items from the parameter source collection that have timestamps before the watermark.
     *
     * Fires window closed events to the parameter {@link WindowClosedHandler} if a window is closed.
     *
     * Windows have inclusive starts and inclusive ends.
     *
     * @param sourceCollection the source collection.
     * @param timestampSelector the user's timestamp selector callback.
     * @param watermarkMs the calling collections watermark in milliseconds.
     * @param windowSizeMs the window size in milliseconds.
     * @param everyMs how frequently a window occurs in milliseconds.
     * @param nextWindowStartTimeMs the next windows start time in milliseconds. First time callers should pass the source collections start time.
     * @param windowClosedHandler the user's window closed handler callback.
     * @return the nextWindowStartTimeMs. This should be saved and reused when calling
     * {@link Utils#performWatermarkedWindowedEviction(List, TimestampSelector, long, long, long, long, WindowClosedHandler)}
     * @param <T> the type of the items in the source collection
     */
    static <T> long performWatermarkedWindowedEviction(
            List<T> sourceCollection,
            TimestampSelector<T> timestampSelector,
            long watermarkMs,
            long windowSizeMs,
            long everyMs,
            long nextWindowStartTimeMs,
            WindowClosedHandler<T> windowClosedHandler) {

        if (sourceCollection == null || sourceCollection.isEmpty()) {
            return nextWindowStartTimeMs;
        }

        if (windowSizeMs <= 0) {
            throw new IllegalArgumentException("windowSizeMs must be > 0");
        }

        if (everyMs <= 0) {
            throw new IllegalArgumentException("everyMs must be > 0");
        }

        int fromIdx = 0;
        int toIdx = 0;

        long windowStartTimeMs = nextWindowStartTimeMs;

        /*
         * Windows are inclusive on both ends.
         */
        long windowEndTimeMs = windowStartTimeMs + windowSizeMs;

        /*
         * Close every window whose inclusive end timestamp has
         * reached the watermark.
         */
        while (windowEndTimeMs <= watermarkMs) {

            /*
             * Find the first element >= window start.
             */
            while (fromIdx < sourceCollection.size()) {
                long timestampMs = timestampSelector.select(sourceCollection.get(fromIdx));

                if (timestampMs >= windowStartTimeMs) {
                    break;
                }

                fromIdx++;
            }

            if (toIdx < fromIdx) {
                toIdx = fromIdx;
            }

            /*
             * toIdx remains EXCLUSIVE for List.subList(),
             * even though the time window itself is inclusive.
             */
            while (toIdx < sourceCollection.size()) {
                long timestampMs = timestampSelector.select(sourceCollection.get(toIdx));

                if (timestampMs > windowEndTimeMs) {
                    break;
                }

                toIdx++;
            }

            List<T> itemsInWindow = new ArrayList<T>(sourceCollection.subList(fromIdx, toIdx));

            SlidingTimeWindow<T> window = new SlidingTimeWindow<T>(windowStartTimeMs, windowEndTimeMs, itemsInWindow);

            // TODO
            if(windowClosedHandler != null) {
                windowClosedHandler.onWindowClosed(window);
            }

            /*
             * Advance to the next sliding window.
             */
            windowStartTimeMs += everyMs;
            windowEndTimeMs = windowStartTimeMs + windowSizeMs;
        }

        /*
         * windowStartTimeMs is now the beginning of the oldest
         * window that has NOT closed -- evict earlier items.
         */
        long evictionTimestampMs = Math.min(windowStartTimeMs, watermarkMs);

        int evictionIdx = 0;

        while (evictionIdx < sourceCollection.size()) {
            long timestampMs = timestampSelector.select(sourceCollection.get(evictionIdx));

            if (timestampMs >= evictionTimestampMs) {
                break;
            }

            evictionIdx++;
        }

        if (evictionIdx > 0) {
            sourceCollection.subList(0, evictionIdx).clear();
        }

        /*
         * Return the start of the next window that has not yet closed.
         */
        return windowStartTimeMs;
    }

    /**
     *
     * @param sourceCollection the source collection.
     * @param timestampSelector the user's timestamp selector callback.
     * @param watermarkMs the calling collections watermark in milliseconds.
     * @param timeoutMs the timeout duration in milliseconds. This duration is used to calculate the gap between events
     *                  in the collection -- the gap exceeding the timeout duration will cause a new session window
     *                  to be created.
     * @param windowClosedHandler the user's window closed handler callback.
     * @param <T> the type of the items in the source collection
     */
    static <T> void performSessionWindowEviction(
            List<T> sourceCollection,
            TimestampSelector<T> timestampSelector,
            long watermarkMs,
            long timeoutMs,
            WindowClosedHandler<T> windowClosedHandler) {

        if (sourceCollection == null || sourceCollection.isEmpty()) {
            return;
        }

        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeoutMs must be > 0");
        }

        int sessionStartIdx = 0;
        int evictionIdx = 0;

        for (int currentIdx = 1; currentIdx < sourceCollection.size(); currentIdx++) {

            long previousTimestampMs = timestampSelector.select(sourceCollection.get(currentIdx - 1));

            long currentTimestampMs = timestampSelector.select(sourceCollection.get(currentIdx));

            long gapMs = currentTimestampMs - previousTimestampMs;

            /*
             * If the gap does not exceed the timeout, both elements
             * belong to the same session.
             */
            if (gapMs <= timeoutMs) {
                continue;
            }

            /*
             * We have discovered the end of a session -- however do not finalize it until the watermark has
             * advanced through its timeout boundary.
             */
            long sessionTimeoutTimeMs = previousTimestampMs + timeoutMs;

            if (sessionTimeoutTimeMs > watermarkMs) {
                break;
            }

            long sessionStartTimeMs = timestampSelector.select(sourceCollection.get(sessionStartIdx));

            long sessionEndTimeMs = previousTimestampMs;

            List<T> itemsInWindow = new ArrayList<T>(sourceCollection.subList(sessionStartIdx, currentIdx));

            SessionTimeWindow<T> window = new SessionTimeWindow<T>(timeoutMs, sessionStartTimeMs, sessionEndTimeMs, itemsInWindow);

            windowClosedHandler.onWindowClosed(window);
            evictionIdx = currentIdx;

            /*
             * currentIdx is now the beginning of the next session.
             */
            sessionStartIdx = currentIdx;
        }

        /*
         * Remove every element belonging exclusively to finalized
         * sessions.
         *
         * The current/open session remains in sourceCollection.
         */
        if (evictionIdx > 0) {
            sourceCollection.subList(0, evictionIdx).clear();
        }
    }

}
