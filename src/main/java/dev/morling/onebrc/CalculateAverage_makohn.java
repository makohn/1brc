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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_makohn {

    private static final String FILE = "./measurements.txt";

    private static class Measurement {
        int min;
        int max;
        int count = 1;
        int sum;

        Measurement(int val) {
            this.min = val;
            this.max = val;
            this.sum = val;
        }

        @Override
        public String toString() {
            return round(min) + "/" + round((1.0 * sum) / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    private static final int CHUNK_SIZE = 1_000_000;

    private record ChunkProcessor(List<String> chunk, int chunkNr) implements Callable<Map<String, Measurement>> {

        @Override
        public Map<String, Measurement> call() {
            final var map = new HashMap<String, Measurement>(CHUNK_SIZE);
            for (final var line : chunk) {
                final var kv = line.split(";");
                final var key = kv[0];
                final var value = toInt(kv[1]);
                if (map.containsKey(key)) {
                    final var current = map.get(key);
                    current.min = Math.min(current.min, value);
                    current.max = Math.max(current.max, value);
                    current.count++;
                    current.sum += value;
                }
                else {
                    map.put(key, new Measurement(value));
                }
            }
            return map;
        }

    }

    static final int[] POWERS_OF_10 = {1, 1, 10, 100, 1000};
    private static int toInt(String s) {
        int res = 0;
        final var len = s.length();
        for (int i = len-1; i >= 0; i--) {
            final var c = s.charAt(i);
            switch (c) {
                case '-' -> res *= -1;
                case '.' -> {}
                default -> res += (c - '0') * POWERS_OF_10[len-i-1];
            }
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        try (final var in = Files.lines(Paths.get(FILE));
             final var executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors())) {
            final var spliterator = in.spliterator();
            final var futures = new ArrayList<Future<Map<String, Measurement>>>();
            int nr = 0;
            while (true) {
                final var chunk = new ArrayList<String>(CHUNK_SIZE);
                int i = 0;
                while (i < CHUNK_SIZE && spliterator.tryAdvance(chunk::add))
                    i++;
                if (chunk.isEmpty())
                    break;
                futures.add(executor.submit(new ChunkProcessor(chunk, nr)));
                nr++;
            }
            final var res = new TreeMap<>(futures.removeFirst().get());
            for (final var future : futures) {
                final var tmp = future.get();
                for (final var entry : tmp.entrySet()) {
                    final var key = entry.getKey();
                    final var value = entry.getValue();
                    if (res.containsKey(key)) {
                        final var cur = res.get(key);
                        cur.min = Math.min(cur.min, value.min);
                        cur.max = Math.max(cur.max, value.max);
                        cur.count += value.count;
                        cur.sum += value.sum;
                    }
                }
            }
            System.out.println(res);
        }
    }
}
