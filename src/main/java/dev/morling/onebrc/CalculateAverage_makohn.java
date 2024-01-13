/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_makohn {

    private static final String FILE = "./measurements.txt";

    private static class Measurement {
        double min;
        double max;
        int count = 1;
        double sum;

        Measurement(double val) {
            this.min = val;
            this.max = val;
            this.sum = val;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static final Map<String, Measurement> res = new TreeMap<>();

    public static void main(String[] args) throws IOException {
        final var in = new BufferedReader(new FileReader(FILE));

        String line;
        while ((line = in.readLine()) != null) {
            final var l = line;
            final var kv = l.split(";");
            final var k = kv[0];
            final var v = Double.parseDouble(kv[1]);
            if (res.containsKey(k)) {
                final var m = res.get(k);
                m.min = Math.min(m.min, v);
                m.max = Math.max(m.max, v);
                m.sum += v;
                m.count++;
            } else {
                res.put(k, new Measurement(v));
            }
        }

        System.out.println(res);
    }
}
