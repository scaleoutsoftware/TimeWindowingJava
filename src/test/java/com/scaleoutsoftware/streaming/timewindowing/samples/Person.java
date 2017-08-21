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

import java.util.ArrayList;

public class Person {
    int _age;
    ArrayList<HeartRate> _heartRates;

    public Person(int age) {
        _age = age;
        _heartRates = new ArrayList<>();
    }

    public void addHeartRate(int rate, long timestamp) {
        _heartRates.add(new HeartRate(timestamp, rate));
    }

    public ArrayList<HeartRate> getHeartRates() {
        return _heartRates;
    }
}
