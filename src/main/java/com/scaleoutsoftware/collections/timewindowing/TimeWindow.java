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
 * Supports iteration over a collection of elements that fall within the given start and end timestamps.
 * @param <T> the object type of the source collection.
 */
public interface TimeWindow<T> extends Iterable<T> {

    /**
     * Returns the start time of this window.
     * @return the start time of the window.
     */
    long getStartTimeMs();

    /**
     * Returns the end time of this window.
     * @return the end time of this window.
     */
    long getEndTimeMs();

    /**
     * Returns the size of the iterable collection.
     * @return the size of the iterable collection.
     */
    int size();

    /**
     * Retrieves the items of the window as a list.
     * @return a list of the items in the window.
     */
    List<T> getWindowContents();
}
