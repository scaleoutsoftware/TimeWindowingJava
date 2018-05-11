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
    static <T> void addTimeOrdered(List<T> source, TimestampSelector<T> selector, T toAdd) {
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
     * @param source the source collection
     * @param selector the timestamp selector used to pull a timestamp from an item
     * @param startTime the start of the collection
     * @param <T> the type of the items in the source collection
     */
    static <T> void performEviction(List<T> source, TimestampSelector<T> selector, long startTime) {
        if(source != null) {
            int from = 0, to = 0;
            boolean clear = false;
            while (to < source.size()) {
                if (selector.select(source.get(to)) < startTime) {
                    to++;
                    clear = true;
                } else {
                    break;
                }
            }

            if (clear)
                source.subList(from, to).clear();
        }
    }

}
