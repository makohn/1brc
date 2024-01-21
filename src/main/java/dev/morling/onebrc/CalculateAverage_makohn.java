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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

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
            return STR."\{round(min)}/\{round((1.0 * sum) / count)}/\{round(max)}";
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }
    }

    // Convert a given byte array of temperature data to an int value
    //
    // Lets say we have the following "byte" array: ['-', '1', '2', '.', '5'] (converted to chars for readability)
    //
    // We reverse-iterate the array adding each digit to an accumulator.
    // But first we multiply by a power of 10 to account for the digit's position.
    //
    // So in the example:
    //
    // res = 0
    // '5' -> res += 5 * 1 -> 5
    // '.' -> ignore
    // '2' -> res += 2 * 10 -> 5 + 20 = 25
    // '1' -> res += 1 * 100 -> 25 + 100 = 125
    // '-' -> res *= -1 -> 125 * -1 = -125
    //
    // Since the temperate values only have one decimal, we can use integer arithmetic until the end
    //
    private static int toInt(byte[] in, int offset) {
        int sign = 1;
        int s = offset;
        if ((in[s] & 0xFF) == '-') {
            sign = -1;
            s++;
        }

        if ((in[s+1] & 0xFF) == '.')
            return sign * (((in[s] & 0xFF) - '0') * 10 + ((in[s+2] & 0xFF) - '0'));

        return sign * (((in[s] & 0xFF) - '0') * 100 + ((in[s+1] & 0xFF) - '0') * 10 + ((in[s+3] & 0xFF) - '0'));
    }

    private static Collection<ByteBuffer> getChunks(MemorySegment memory, long chunkSize, long fileSize) {
        final var chunks = new ArrayList<ByteBuffer>();
        var chunkStart = 0L;
        var chunkEnd = 0L;
        while (chunkStart < fileSize) {
            chunkEnd = Math.min((chunkStart + chunkSize), fileSize);
            // starting from the calculated chunkEnd, seek the next newline to get the real chunkEnd
            while (chunkEnd < fileSize && (memory.getAtIndex(ValueLayout.JAVA_BYTE, chunkEnd) & 0xFF) != '\n')
                chunkEnd++;
            // we have found our chunk boundaries, add a slice of memory with these boundaries to our list of chunks
            chunks.add(memory.asSlice(chunkStart, chunkEnd - chunkStart).asByteBuffer());
            // next chunk
            chunkStart = chunkEnd + 1;
        }
        return chunks;
    }

    // Station name: <= 100 bytes
    // Temperature: <= 5 bytes
    //
    // Semicolon and new line are ignored
    private static final int MAX_BYTES_PER_ROW = 105;

    private static Map<String, Measurement> processChunk(ByteBuffer chunk) {
        final var map = new HashMap<String, Measurement>();
        final var buffer = new byte[MAX_BYTES_PER_ROW];
        var i = 0;
        var delimiter = 0;
        // Process the chunk byte by byte and store each line in buffer
        while (chunk.hasRemaining()) {
            final var c = chunk.get();
            switch (c & 0xFF) {
                // Memorize the position of the semicolon, such that we can divide the buffer afterward
                case ';' -> delimiter = i;
                // If we encounter newline, we can do the actual calculations for the current line
                case '\n' -> {
                    final var key = new String(buffer, 0, delimiter, StandardCharsets.UTF_8);
                    final var value = toInt(buffer, delimiter);
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
                    i = 0;
                    delimiter = 0;
                }
                default -> {
                    buffer[i] = c;
                    i++;
                }
            }
        }
        return map;
    }

    public static void main(String[] args) throws Exception {
        final var numProcessors = Runtime.getRuntime().availableProcessors();
        // memory-map the input file
        try (final var channel = FileChannel.open(Paths.get(FILE), StandardOpenOption.READ)) {
            final var fileSize = channel.size();
            final var chunkSize = (fileSize / numProcessors);
            final var mappedMemory = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
            // process the mapped data concurrently in chunks. Each chunk is processed on a dedicated thread
            final var chunks = getChunks(mappedMemory, chunkSize, fileSize);
            final var processed = chunks
                    .parallelStream()
                    .map(CalculateAverage_makohn::processChunk)
                    .collect(Collectors.toList()); // materialize and thus synchronize
            // merge the results, we can initialize with the first result, to avoid redundant key-checks
            final var first = processed.removeFirst();
            final var res = processed
                    .stream()
                    .reduce(new TreeMap<>(first), (acc, partial) -> {
                        for (final var entry : partial.entrySet()) {
                            final var key = entry.getKey();
                            final var value = entry.getValue();
                            if (acc.containsKey(key)) {
                                final var cur = acc.get(key);
                                cur.min = Math.min(cur.min, value.min);
                                cur.max = Math.max(cur.max, value.max);
                                cur.count += value.count;
                                cur.sum += value.sum;
                            }
                            else {
                                acc.put(key, value);
                            }
                        }
                        return acc;
                    });
            System.out.println(res);
        }
    }
}
