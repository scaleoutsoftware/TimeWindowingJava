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
 * The WindowClosedHandler event handler is used to handle closed windows from a windowing collection.
 * @param <T> the type of the objects in the source collection.
 */
public interface WindowClosedHandler<T> {
    /**
     * The onWindowClosed event is fired when a Windowed collection has closed a window after the watermark of the
     * collection exceeds the end time of the window.
     * @param window the closed window.
     */
    public void onWindowClosed(TimeWindow<T> window);
}
