/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.DirectAccessInput;
import org.elasticsearch.simdvec.MemorySegmentAccessInputAccess;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StoreMetricsIndexInput extends FilterIndexInput implements DirectAccessInput {
    /**
     * Task-scoped {@link StoreMetrics} for the current thread, set by {@code StoreMetricsAwareExecutor}
     * (parallel-collection workers) and by {@code SearchService} (single-slice search thread) before
     * a search phase begins and cleared in a {@code finally} block when the phase exits.
     *
     * <p>When set, {@link #addBytesRead} and {@link #randomAccessSlice} use this value directly,
     * avoiding a virtual dispatch through {@link PluggableDirectoryMetricsHolder#instance()} on every
     * DocValues acquisition. When null (merge threads, pre-phase {@code openInput}, unbound paths),
     * the code falls back to {@code metricHolder.instance()}.
     *
     * <p>Only bind a real {@link StoreMetrics} here — never the NOOP subtype — or the direct field
     * write {@code m.bytesRead += bytes} will make NOOP counters visible in
     * {@code buildDirectoryMetricsDelta()}.
     */
    public static final ThreadLocal<StoreMetrics> CURRENT_METRICS = new ThreadLocal<>();

    final PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder;
    // package-private so randomAccessSlice() can pre-populate child.cachedMetrics without a setter
    StoreMetrics cachedMetrics;

    public static IndexInput create(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
        if (in instanceof StoreMetricsIndexInput) {
            // annoyingly, source-only snapshots do this for linked files.
            return in;
        } else if (in instanceof RandomAccessInput) {
            return new RandomAccessIndexInput(resourceDescription, in, metricHolder);
        } else {
            return new StoreMetricsIndexInput(resourceDescription, in, metricHolder);
        }
    }

    private StoreMetricsIndexInput(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
        super(resourceDescription, in);
        this.metricHolder = metricHolder;
        assert in instanceof StoreMetricsIndexInput == false;
    }

    @Override
    public byte readByte() throws IOException {
        byte result = in.readByte();
        addBytesRead(1);
        return result;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        in.readBytes(b, offset, len);
        addBytesRead(len);
    }

    IndexInput createCopy(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
        return new StoreMetricsIndexInput(resourceDescription, in, metricHolder);
    }

    @Override
    public IndexInput clone() {
        return createCopy(toString(), in.clone(), metricHolder.singleThreaded());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return createCopy(sliceDescription, in.slice(sliceDescription, offset, length), metricHolder.singleThreaded());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length, IOContext context) throws IOException {
        return createCopy(sliceDescription, in.slice(sliceDescription, offset, length, context), metricHolder.singleThreaded());
    }

    @Override
    public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
        RandomAccessInput delegate = in.randomAccessSlice(offset, length);
        // Pre-populate the child's cachedMetrics from the task-scoped ThreadLocal (when bound) so the
        // child avoids a metricHolder.instance() call on its first read. When CURRENT_METRICS is null
        // (merge threads, pre-phase openInput, unbound paths), leave cachedMetrics null — addBytesRead
        // will resolve it on the reading thread via metricHolder.instance().
        //
        // Never mutate this.cachedMetrics here — the parent IndexInput (e.g. DocValues producer data)
        // is shared across concurrent searches; writing to its field from randomAccessSlice() is an
        // unsynchronised data race.
        StoreMetrics current = CURRENT_METRICS.get();
        if (delegate instanceof IndexInput input) {
            RandomAccessIndexInput copy = new RandomAccessIndexInput(input.toString(), input, metricHolder.singleThreaded());
            copy.cachedMetrics = current;   // null when unbound — resolved lazily on first read
            return copy;
        } else {
            return new MetricsRandomAccessInput(delegate, metricHolder.singleThreaded());
        }
    }

    @Override
    public void prefetch(long offset, long length) throws IOException {
        in.prefetch(offset, length);
    }

    @Override
    public boolean withMemorySegmentSlice(long offset, long length, CheckedConsumer<MemorySegment, IOException> action) throws IOException {
        if (in instanceof DirectAccessInput dai) {
            return dai.withMemorySegmentSlice(offset, length, action);
        }
        return false;
    }

    @Override
    public boolean withMemorySegmentSlices(long[] offsets, int length, int count, CheckedConsumer<MemorySegment[], IOException> action)
        throws IOException {
        if (in instanceof DirectAccessInput dai) {
            return dai.withMemorySegmentSlices(offsets, length, count, action);
        }
        return false;
    }

    @Override
    public Optional<Boolean> isLoaded() {
        return in.isLoaded();
    }

    @Override
    public void updateIOContext(IOContext context) throws IOException {
        in.updateIOContext(context);
    }

    void addBytesRead(long bytes) {
        StoreMetrics m = cachedMetrics;
        if (m == null) {
            m = CURRENT_METRICS.get();
            if (m == null) m = metricHolder.instance();
            if (m == null) return;
            if (m.getClass() != StoreMetrics.class) {
                // Subclass (e.g. NOOP): honour virtual dispatch so overrides like the empty
                // addBytesRead() still apply. Do not cache — the next call re-resolves.
                m.addBytesRead(bytes);
                return;
            }
            cachedMetrics = m;
        }
        m.bytesRead += bytes;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
        in.readBytes(b, offset, len, useBuffer);
        addBytesRead(len);
    }

    @Override
    public short readShort() throws IOException {
        short result = in.readShort();
        addBytesRead(2);
        return result;
    }

    @Override
    public int readInt() throws IOException {
        int result = getDelegate().readInt();
        addBytesRead(4);
        return result;
    }

    @Override
    public int readVInt() throws IOException {
        long position = in.getFilePointer();
        int result = in.readVInt();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public int readZInt() throws IOException {
        long position = in.getFilePointer();
        int result = in.readZInt();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public long readLong() throws IOException {
        long result = getDelegate().readLong();
        addBytesRead(8);
        return result;
    }

    @Override
    public void readLongs(long[] dst, int offset, int length) throws IOException {
        getDelegate().readLongs(dst, offset, length);
        addBytesRead(8L * length);
    }

    @Override
    public void readInts(int[] dst, int offset, int length) throws IOException {
        getDelegate().readInts(dst, offset, length);
        addBytesRead(4L * length);
    }

    @Override
    public void readFloats(float[] floats, int offset, int len) throws IOException {
        getDelegate().readFloats(floats, offset, len);
        addBytesRead(4L * len);
    }

    @Override
    public long readVLong() throws IOException {
        long position = in.getFilePointer();
        long result = in.readVLong();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public long readZLong() throws IOException {
        long position = in.getFilePointer();
        long result = in.readZLong();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);

        return result;
    }

    @Override
    public String readString() throws IOException {
        long position = in.getFilePointer();
        String result = in.readString();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public Map<String, String> readMapOfStrings() throws IOException {
        long position = in.getFilePointer();
        Map<String, String> result = in.readMapOfStrings();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    @Override
    public Set<String> readSetOfStrings() throws IOException {
        long position = in.getFilePointer();
        Set<String> result = in.readSetOfStrings();
        long bytes = in.getFilePointer() - position;
        assert bytes > 0;
        addBytesRead(bytes);
        return result;
    }

    private static class RandomAccessIndexInput extends StoreMetricsIndexInput
        implements
            RandomAccessInput,
            MemorySegmentAccessInputAccess {
        private final RandomAccessInput delegate;

        private RandomAccessIndexInput(
            String resourceDescription,
            IndexInput in,
            PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder
        ) {
            super(resourceDescription, in, metricHolder);
            assert in instanceof RandomAccessInput;
            this.delegate = (RandomAccessInput) in;
        }

        @Override
        public MemorySegmentAccessInput get() {
            return delegate instanceof MemorySegmentAccessInput ms ? ms : null;
        }

        @Override
        IndexInput createCopy(String resourceDescription, IndexInput in, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
            return new RandomAccessIndexInput(resourceDescription, in, metricHolder);
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public byte readByte(long pos) throws IOException {
            byte result = delegate.readByte(pos);
            addBytesRead(1);
            return result;
        }

        @Override
        public short readShort(long pos) throws IOException {
            short result = delegate.readShort(pos);
            addBytesRead(2);
            return result;
        }

        @Override
        public int readInt(long pos) throws IOException {
            int result = delegate.readInt(pos);
            addBytesRead(4);
            return result;
        }

        @Override
        public long readLong(long pos) throws IOException {
            long result = delegate.readLong(pos);
            addBytesRead(8);
            return result;
        }

        @Override
        public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
            delegate.readBytes(pos, bytes, offset, length);
            addBytesRead(length);
        }
    }

    private static class MetricsRandomAccessInput implements RandomAccessInput {
        private final PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder;
        private final RandomAccessInput delegate;

        private MetricsRandomAccessInput(RandomAccessInput delegate, PluggableDirectoryMetricsHolder<StoreMetrics> metricHolder) {
            this.delegate = delegate;
            this.metricHolder = metricHolder;
        }

        @Override
        public long length() {
            return delegate.length();
        }

        @Override
        public byte readByte(long pos) throws IOException {
            byte result = delegate.readByte(pos);
            metricHolder.instance().addBytesRead(1);
            return result;
        }

        @Override
        public short readShort(long pos) throws IOException {
            short result = delegate.readShort(pos);
            metricHolder.instance().addBytesRead(2);
            return result;
        }

        @Override
        public int readInt(long pos) throws IOException {
            int result = delegate.readInt(pos);
            metricHolder.instance().addBytesRead(4);
            return result;
        }

        @Override
        public long readLong(long pos) throws IOException {
            long result = delegate.readLong(pos);
            metricHolder.instance().addBytesRead(8);
            return result;
        }

        @Override
        public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
            delegate.readBytes(pos, bytes, offset, length);
            metricHolder.instance().addBytesRead(length);
        }
    }
}
