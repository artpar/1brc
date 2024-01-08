
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

//import io.unlogged.Unlogged;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class CalculateAverage_artpar {
    public static final int N_THREADS = 8;
    private static final String FILE = "./measurements.txt";
    private static final int HASH_CONST = 0x9e3779b9; // a constant used in the hash computation
    // private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    // final int VECTOR_SIZE = 512;
    // final int VECTOR_SIZE_1 = VECTOR_SIZE - 1;
    final int AVERAGE_CHUNK_SIZE = 1024 * 256;
    final int AVERAGE_CHUNK_SIZE_1 = AVERAGE_CHUNK_SIZE - 1;
    // ExecutorService threadPool = Executors.newFixedThreadPool(N_THREADS);
    // ExecutorService anotherThreadPool = Executors.newFixedThreadPool(N_THREADS);

    public CalculateAverage_artpar() throws IOException {
        long start = Instant.now().toEpochMilli();
        Path measurementFile = Paths.get(FILE);
        long fileSize = Files.size(measurementFile);

        // System.out.println("File size - " + fileSize);
        int expectedChunkSize = Math.toIntExact(Math.min(fileSize / N_THREADS, Integer.MAX_VALUE / 2));

        ExecutorService threadPool = Executors.newFixedThreadPool(N_THREADS);

        long chunkStartPosition = 0;
        RandomAccessFile fis = new RandomAccessFile(measurementFile.toFile(), "r");
        List<Future<Map<String, MeasurementAggregator>>> futures = new ArrayList<>();
        long bytesReadCurrent = 0;

        FileChannel fileChannel = FileChannel.open(measurementFile, StandardOpenOption.READ);
        while (chunkStartPosition < fileSize) {

            // if (bytesReadCurrent % (1024 * 1024) == 0) {
            // System.out.println("Read " + bytesReadCurrent);
            // }

            long chunkSize = expectedChunkSize;
            chunkSize = fis.skipBytes(Math.toIntExact(chunkSize));

            bytesReadCurrent += chunkSize;
            while (((char) fis.read()) != '\n' && bytesReadCurrent < fileSize) {
                chunkSize++;
                bytesReadCurrent++;
            }

            System.out.println("[" + chunkStartPosition + "] - [" + (chunkStartPosition + chunkSize) + " bytes");
            if (chunkStartPosition + chunkSize >= fileSize) {
                chunkSize = (int) Math.min(fileSize - chunkStartPosition, chunkSize);
            }
            if (chunkSize < 1) {
                break;
            }
            if (chunkSize > Integer.MAX_VALUE) {
                chunkSize = Integer.MAX_VALUE;
            }

            // MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStartPosition,
            // chunkSize);

            ReaderRunnable readerRunnable = new ReaderRunnable(chunkStartPosition, Math.toIntExact(chunkSize),
                    fileChannel);
            Future<Map<String, MeasurementAggregator>> future = threadPool.submit(readerRunnable::run);
            System.out.println("Added future [" + chunkStartPosition + "][" + chunkSize + "]");
            futures.add(future);
            chunkStartPosition += chunkSize + 1;
        }

        fis.close();

        Map<String, MeasurementAggregator> globalMap = futures.parallelStream()
                .flatMap(future -> {
                    try {
                        return future.get().entrySet().stream();
                    }
                    catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }).collect(Collectors.toMap(
                        Map.Entry::getKey, Map.Entry::getValue,
                        MeasurementAggregator::combine));
        fileChannel.close();

        Map<String, ResultRow> results = globalMap.entrySet().stream().parallel()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().finish()));

        threadPool.shutdown();
        Map<String, ResultRow> measurements = new TreeMap<>(results);

        PrintStream printStream = new PrintStream(new BufferedOutputStream(System.out));
        // PrintStream printStream = System.out;
        printStream.print("{");

        boolean isFirst = true;
        for (Map.Entry<String, ResultRow> stringResultRowEntry : measurements.entrySet()) {
            if (!isFirst) {
                printStream.print(", ");
                printStream.flush();
            }
            printStream.flush();
            printStream.print(stringResultRowEntry.getKey());
            printStream.flush();
            printStream.print("=");
            printStream.flush();
            stringResultRowEntry.getValue().printTo(printStream);
            printStream.flush();
            isFirst = false;
        }

        System.out.print("}\n");

        // long end = Instant.now().toEpochMilli();
        // System.out.println((end - start) / 1000);

    }

    // @Unlogged
    public static void main(String[] args) throws IOException {
        new CalculateAverage_artpar();
    }

    private record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min / 10) + "/" + round(mean / 10) + "/" + round(max / 10);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        public void printTo(PrintStream out) {
            out.printf("%.1f/%.1f/%.1f", min / 10, mean / 10, max / 10);
        }
    }

    static class StationName {
        public final int hash;
        private final byte[] nameBytes;
        public MeasurementAggregator measurementAggregator;

        public StationName(byte[] nameBytes, int hash, int doubleValue) {
            this.nameBytes = nameBytes;
            this.hash = hash;
            measurementAggregator = new MeasurementAggregator(doubleValue, doubleValue, doubleValue, 1);
        }

    }

    private static class MeasurementAggregator {
        private int min;
        private int max;
        private double sum;
        private long count;

        public MeasurementAggregator(int min, int max, double sum, long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        MeasurementAggregator combine(MeasurementAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
            return this;
        }

        void combine(int value) {
            min = min < value ? min : value;
            max = max > value ? max : value;
            sum += value;
            count += 1;
        }

        ResultRow finish() {
            double mean = (count > 0) ? sum / count : 0;
            return new ResultRow(min, mean, max);
        }
    }

    private class ReaderRunnable {
        private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_MAX;
        private final long startPosition;
        private final int chunkSize;
        private final FileChannel fileChannel;
        StationNameMap stationNameMap = new StationNameMap();

        private ReaderRunnable(long startPosition, int chunkSize, FileChannel fileChannel) {
            this.startPosition = startPosition;
            this.chunkSize = chunkSize;
            this.fileChannel = fileChannel;
        }

        public Map<String, MeasurementAggregator> run() throws IOException {
            // System.out.println("Started " + startPosition);
            MemorySegment memorySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY,
                    startPosition, chunkSize, Arena.ofConfined());

            // MappedByteBuffer mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPosition,
            // chunkSize);

            // System.out.println("start reading " + startPosition);
            int position = 0;
            int hash = 0;
            int isNegative;

            int lineStartIndex = 0;
            int semicolonOffset = 0;
            int newLineOffset = 0;
            int valueStartOffset = 0;

            try {

                int value = 0;

                int sizeToProcessByVector = BYTE_SPECIES.loopBound(chunkSize) - 32;
                if (sizeToProcessByVector > 0) {
                    ByteVector line;
                    while (position < sizeToProcessByVector) {
                        newLineOffset = 0;
                        int offset;
                        line = ByteVector.fromMemorySegment(BYTE_SPECIES, memorySegment, lineStartIndex,
                                ByteOrder.nativeOrder());

                        semicolonOffset = offset = line.compare(VectorOperators.EQ, ';').firstTrue();
                        while (offset == line.length()) {
                            position += line.length();
                            line = ByteVector.fromMemorySegment(BYTE_SPECIES, memorySegment, position,
                                    ByteOrder.nativeOrder());
                            newLineOffset += line.length();
                            offset = line.compare(VectorOperators.EQ, ';').firstTrue();
                            semicolonOffset += offset;
                        }
                        semicolonOffset += lineStartIndex;
                        newLineOffset += (offset = line.compare(VectorOperators.EQ, '\n').firstTrue());
                        while (offset == line.length()) {
                            position += line.length();
                            line = ByteVector.fromMemorySegment(BYTE_SPECIES, memorySegment, position,
                                    ByteOrder.nativeOrder());
                            offset = line.compare(VectorOperators.EQ, '\n').firstTrue();
                            newLineOffset += offset;
                        }
                        newLineOffset = lineStartIndex + newLineOffset;
                        // MappedByteBuffer row = mappedByteBuffer.slice(lineStartIndex,
                        // newLineOffset - lineStartIndex);

                        // while ((b = memorySegment.get(ValueLayout.JAVA_BYTE, position++)) != ';') {
                        // hash ^= (b & 0xff); // XOR the byte with the hash
                        // hash = (hash << 5) | (hash >>> -5); // Rotate left to mix the bits
                        // hash ^= HASH_CONST; // XOR with the constant to further mix
                        // }

                        // semicolonOffset = position - 1;
                        valueStartOffset = semicolonOffset + 1;
                        isNegative = memorySegment.get(ValueLayout.JAVA_BYTE,
                                valueStartOffset) == '-' && valueStartOffset++ > 0 ? -1 : 1;
                        // while (memorySegment.get(ValueLayout.JAVA_BYTE, position++) != '\n' && position < chunkSize) {
                        // }
                        // newLineOffset = position - 1;

                        int nameLength = semicolonOffset - lineStartIndex;
                        MemorySegment nameSlice = memorySegment.asSlice(lineStartIndex, nameLength);
                        hash = Arrays.hashCode(nameSlice.toArray(ValueLayout.JAVA_BYTE));
                        long valueLength = newLineOffset - 1 - valueStartOffset;
                        MemorySegment valueSlice = memorySegment.asSlice(valueStartOffset, valueLength + 1);

                        if (valueSlice.get(ValueLayout.JAVA_BYTE, 1) == '.') {
                            value = isNegative * (valueSlice.get(ValueLayout.JAVA_BYTE, 0) * 10 +
                                    valueSlice.get(ValueLayout.JAVA_BYTE, 2) - 528);
                        }
                        else {
                            value = isNegative * (valueSlice.get(ValueLayout.JAVA_BYTE, 0) * 100 +
                                    valueSlice.get(ValueLayout.JAVA_BYTE, 1) * 10 +
                                    valueSlice.get(ValueLayout.JAVA_BYTE, 3) - 5328);
                        }

                        stationNameMap.getOrCreate(nameSlice, nameLength, value, hash);
                        position = newLineOffset + 1;
                        lineStartIndex = position;
                    }

                }

                byte[] rawBuffer = new byte[200];
                byte b;
                byte one;
                int rawBufferReadIndex = 0;
                while (position < chunkSize) {

                    b = memorySegment.get(ValueLayout.JAVA_BYTE, position++);
                    rawBuffer[rawBufferReadIndex++] = b;
                    hash ^= (b & 0xff); // XOR the byte with the hash
                    hash = (hash << 5) | (hash >>> -5); // Rotate left to mix the bits
                    hash ^= HASH_CONST; // XOR with the constant to further mix

                    if (b != ';') {
                        continue;
                    }
                    int result = memorySegment.get(ValueLayout.JAVA_BYTE, position++) * 10;
                    isNegative = result == 450 && ((result = memorySegment.get(ValueLayout.JAVA_BYTE,
                            position++) * 10) == 0 || true) ? -1 : 1;
                    result = isNegative * ((one = memorySegment.get(ValueLayout.JAVA_BYTE,
                            position++)) == '.' ? result + memorySegment.get(ValueLayout.JAVA_BYTE, position++) - 528
                                    : result * 10 + one * 10 + (position++ * 0) +
                                            memorySegment.get(ValueLayout.JAVA_BYTE, position++) - 5328);
                    position++;
                    stationNameMap.getOrCreate(rawBuffer, rawBufferReadIndex - 1, result, hash);
                    hash = 0;
                    rawBufferReadIndex = 0;
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                //
            }
            long end = System.currentTimeMillis();
            // System.out.println("completed reading " + position + " - " + (end - start) / 1000);
            return Arrays.stream(stationNameMap.names).parallel().filter(Objects::nonNull)
                    .collect(Collectors.toMap(e -> new String(e.nameBytes), e -> e.measurementAggregator,
                            MeasurementAggregator::combine));

        }
    }

    class StationNameMap {
        int[] indexes = new int[AVERAGE_CHUNK_SIZE];
        StationName[] names = new StationName[AVERAGE_CHUNK_SIZE];
        int currentIndex = 0;

        public void getOrCreate(byte[] stationNameBytes, int length, int doubleValue, int hash) {
            int position = hash & AVERAGE_CHUNK_SIZE_1;
            while (indexes[position] != 0 && names[indexes[position]].hash != hash) {
                position = ++position & AVERAGE_CHUNK_SIZE_1;
            }
            StationName stationName;
            if (indexes[position] != 0) {
                names[indexes[position]].measurementAggregator.combine(doubleValue);
            }
            else {
                byte[] destination = new byte[length];
                System.arraycopy(stationNameBytes, 0, destination, 0, length);
                stationName = new StationName(destination, hash, doubleValue);
                indexes[position] = ++currentIndex;
                names[indexes[position]] = stationName;
            }
        }

        public void getOrCreate(MemorySegment nameSlice, int length, int doubleValue, int hash) {
            int position = hash & AVERAGE_CHUNK_SIZE_1;
            while (indexes[position] != 0 && names[indexes[position]].hash != hash) {
                position = ++position & AVERAGE_CHUNK_SIZE_1;
            }
            StationName stationName;
            if (indexes[position] != 0) {
                names[indexes[position]].measurementAggregator.combine(doubleValue);
            }
            else {
                byte[] destination = new byte[length];
                System.arraycopy(nameSlice.toArray(ValueLayout.JAVA_BYTE), 0, destination, 0, length);
                stationName = new StationName(destination, hash, doubleValue);
                indexes[position] = ++currentIndex;
                names[indexes[position]] = stationName;
            }

        }
    }

}
