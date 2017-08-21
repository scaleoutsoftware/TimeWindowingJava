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
package com.scaleoutsoftware.streaming.timewindowing.samples;


public class HeartRate {
    long _timestamp;
    int _heartRate;

    public HeartRate(long timestamp, int heartRate) {
        _timestamp = timestamp;
        _heartRate = heartRate;
    }

    public long getTimestamp() {
        return _timestamp;
    }

    public int getHeartRate() {
        return _heartRate;
    }
}
