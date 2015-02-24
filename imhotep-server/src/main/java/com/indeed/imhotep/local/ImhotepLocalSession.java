/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.local;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.FlamdexOutOfMemoryException;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.IntValueLookup;
import com.indeed.flamdex.api.RawFlamdexReader;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.api.StringValueLookup;
import com.indeed.flamdex.datastruct.FastBitSet;
import com.indeed.flamdex.datastruct.FastBitSetPooler;
import com.indeed.flamdex.fieldcache.ByteArrayIntValueLookup;
import com.indeed.flamdex.fieldcache.IntArrayIntValueLookup;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.reader.FlamdexMetadata;
import com.indeed.flamdex.search.FlamdexSearcher;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.utils.FlamdexUtils;
import com.indeed.imhotep.AbstractImhotepSession;
import com.indeed.imhotep.FTGSSplitter;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.ImhotepMemoryPool;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.MemoryReserver;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.group.ImhotepChooser;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.metrics.AbsoluteValue;
import com.indeed.imhotep.metrics.Addition;
import com.indeed.imhotep.metrics.CachedInterleavedMetrics;
import com.indeed.imhotep.metrics.CachedMetric;
import com.indeed.imhotep.metrics.Constant;
import com.indeed.imhotep.metrics.Count;
import com.indeed.imhotep.metrics.DelegatingMetric;
import com.indeed.imhotep.metrics.Division;
import com.indeed.imhotep.metrics.Equal;
import com.indeed.imhotep.metrics.Exponential;
import com.indeed.imhotep.metrics.GreaterThan;
import com.indeed.imhotep.metrics.GreaterThanOrEqual;
import com.indeed.imhotep.metrics.LessThan;
import com.indeed.imhotep.metrics.LessThanOrEqual;
import com.indeed.imhotep.metrics.Log;
import com.indeed.imhotep.metrics.Log1pExp;
import com.indeed.imhotep.metrics.Logistic;
import com.indeed.imhotep.metrics.Max;
import com.indeed.imhotep.metrics.Min;
import com.indeed.imhotep.metrics.Modulus;
import com.indeed.imhotep.metrics.Multiplication;
import com.indeed.imhotep.metrics.MultiplyAndShiftRight;
import com.indeed.imhotep.metrics.NotEqual;
import com.indeed.imhotep.metrics.ShiftLeftAndDivide;
import com.indeed.imhotep.metrics.Subtraction;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.imhotep.service.CachedFlamdexReader;
import com.indeed.imhotep.service.RawCachedFlamdexReader;
import com.indeed.util.core.Pair;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.core.threads.ThreadSafeBitSet;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class isn't even close to remotely thread safe, do not use it
 * simultaneously from multiple threads
 */
public final class ImhotepLocalSession extends AbstractImhotepSession {
    static final Logger log = Logger.getLogger(ImhotepLocalSession.class);

    static final boolean logTiming;

    static {
        logTiming =
                "true".equals(System.getProperty("com.indeed.imhotep.local.ImhotepLocalSession.logTiming"));
    }

    private static final int MAX_NUMBER_STATS = 64;
    static final int BUFFER_SIZE = 2048;

    private int numDocs;
    // buffers that will be reused to avoid excessive allocations
    final int[] docIdBuf = new int[BUFFER_SIZE];
    final long[] valBuf = new long[BUFFER_SIZE];
    final int[] docGroupBuffer = new int[BUFFER_SIZE];

    // do not close flamdexReader, it is separately refcounted
    private FlamdexReader flamdexReader;
    private SharedReference<FlamdexReader> flamdexReaderRef;
    private FlamdexReader originalReader;
    private SharedReference<FlamdexReader> originalReaderRef;

    private final String optimizedIndexesDir;

    final MemoryReservationContext memory;

    GroupLookup docIdToGroup;

    int[] groupDocCount;

    int numStats;
    private long[][] groupStats = new long[MAX_NUMBER_STATS][];
    final IntValueLookup[] statLookup = new IntValueLookup[MAX_NUMBER_STATS];
    private final List<String> statCommands;

    private final boolean[] needToReCalcGroupStats = new boolean[MAX_NUMBER_STATS];

    private boolean closed = false;
    @VisibleForTesting
    private Map<String, DynamicMetric> dynamicMetrics = Maps.newHashMap();

    Map<Pair<String, Boolean>, FastBitSet> fieldZeroDocBitsets;

    private final Exception constructorStackTrace;

    private final File optimizationLog;

    private FTGSSplitter ftgsIteratorSplits;

    public ImhotepLocalSession(final FlamdexReader flamdexReader) throws ImhotepOutOfMemoryException {
        this(flamdexReader, null,
                new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)), false);
    }

    public ImhotepLocalSession(FlamdexReader flamdexReader, boolean optimizeGroupZeroLookups) throws ImhotepOutOfMemoryException {
        this(flamdexReader, null,
                new MemoryReservationContext(new ImhotepMemoryPool(Long.MAX_VALUE)),
                optimizeGroupZeroLookups);
    }

    public ImhotepLocalSession(final FlamdexReader flamdexReader,
                               String optimizedIndexDirectory,
                               final MemoryReservationContext memory,
                               boolean optimizeGroupZeroLookups) throws ImhotepOutOfMemoryException {
        constructorStackTrace = new Exception();
        flamdexReaderRef = SharedReference.create(flamdexReader);
        this.flamdexReader = flamdexReader;
        this.memory = memory;
        this.numDocs = flamdexReader.getNumDocs();
        this.optimizedIndexesDir = optimizedIndexDirectory;

        if (!memory.claimMemory(BUFFER_SIZE * (4 + 4 + 4) + 12 * 2)) {
            throw new ImhotepOutOfMemoryException();
        }

        docIdToGroup = new ConstantGroupLookup(this, 1, numDocs);
        docIdToGroup.recalculateNumGroups();
        groupDocCount = clearAndResize((int[]) null, docIdToGroup.getNumGroups(), memory);
        groupDocCount[1] = numDocs;
        this.statCommands = new ArrayList<String>();

        this.optimizationLog =
                new File(this.optimizedIndexesDir, UUID.randomUUID().toString()
                        + ".optimization_log");

        if (optimizeGroupZeroLookups) {
            fieldZeroDocBitsets = Maps.newHashMap();
        } else {
            fieldZeroDocBitsets = null;
        }
    }

    FlamdexReader getReader() {
        return this.flamdexReader;
    }

    public Map<String, DynamicMetric> getDynamicMetrics() {
        return dynamicMetrics;
    }

    int getNumDocs() {
        return this.numDocs;
    }

    /*
     * record structure to store the info necessary to rebuild the
     * DynamicMetrics after one or more optimizes and a reset
     */
    public static class OptimizationRecord implements Serializable {
        private static final long serialVersionUID = 1L;
        public long time;
        List<String> intFieldsMerged;
        List<String> stringFieldsMerged;
        String shardLocation;
        List<ShardMergeInfo> mergedShards;
    }

    public static class ShardMergeInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        int numDocs;
        Map<String, DynamicMetric> dynamicMetrics;
        int[] newDocIdToOldDocId;
    }

    /*
     * Finds a good place to store the new, optimized shard and opens a
     * SimpleFlamdexWriter to it.
     */
    private SimpleFlamdexWriter createNewTempWriter(int maxDocs) throws IOException {
        final File tempIdxDir;
        final String newShardName;
        final File newShardDir;

        newShardName = "temp." + UUID.randomUUID().toString();
        tempIdxDir = new File(this.optimizedIndexesDir);
        newShardDir = new File(tempIdxDir, newShardName);
        newShardDir.mkdir();

        return new SimpleFlamdexWriter(newShardDir.getCanonicalPath(), maxDocs);
    }

    /* wrapper for SimpleFlamdexReader which deletes the on disk data on close() */
    private static class AutoDeletingReader extends SimpleFlamdexReader {

        public AutoDeletingReader(String directory,
                                  int numDocs,
                                  Collection<String> intFields,
                                  Collection<String> stringFields,
                                  boolean useMMapMetrics) {
            super(directory, numDocs, intFields, stringFields, useMMapMetrics);
        }

        public static AutoDeletingReader open(String directory) throws IOException {
            return open(directory, new Config());
        }

        public static AutoDeletingReader open(String directory, Config config) throws IOException {
            final FlamdexMetadata metadata = FlamdexMetadata.readMetadata(directory);
            final Collection<String> intFields = scan(directory, ".intterms");
            final Collection<String> stringFields = scan(directory, ".strterms");
            if (config.isWriteBTreesIfNotExisting()) {
                buildIntBTrees(directory, Lists.newArrayList(intFields));
                buildStringBTrees(directory, Lists.newArrayList(stringFields));
            }
            return new AutoDeletingReader(directory, metadata.numDocs, intFields, stringFields,
                                          config.isUseMMapMetrics());
        }

        @Override
        public void close() throws IOException {
            File dir = new File(this.directory);
            super.close();

            FileUtils.deleteDirectory(dir);
        }

    }

    /* Tweak to ObjectOutputStream which allows it to append to an existing file */
    public static class AppendingObjectOutputStream extends ObjectOutputStream {

        public AppendingObjectOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        @Override
        protected void writeStreamHeader() throws IOException {
            // do not write a header, but reset:
            reset();
        }

    }

    @Override
    public synchronized void rebuildAndFilterIndexes(@Nonnull final List<String> intFields,
                                                     @Nonnull final List<String> stringFields) throws ImhotepOutOfMemoryException {
        final IndexReWriter rewriter;
        final ObjectOutputStream oos;
        final SimpleFlamdexWriter w;
        final ArrayList<String> statsCopy;

        long time = System.currentTimeMillis();

        /* pop off all the stats, they will be repushed after the optimization */
        statsCopy = new ArrayList<String>(this.statCommands);
        while (this.numStats > 0) {
            this.popStat();
        }
        this.statCommands.clear();

        MemoryReservationContext rewriterMemory = new MemoryReservationContext(memory);
        rewriter = new IndexReWriter(Arrays.asList(this), this, rewriterMemory);
        try {
            w = createNewTempWriter(this.numDocs);
            rewriter.optimizeIndecies(intFields, stringFields, w);
            w.close();

            /*
             * save a record of the merge, so it can be unwound later if the
             * shards are reset
             */
            if (this.optimizationLog.exists()) {
                /* plain ObjectOutputStream does not append correctly */
                oos =
                        new AppendingObjectOutputStream(new FileOutputStream(this.optimizationLog,
                                                                             true));
            } else {
                oos = new ObjectOutputStream(new FileOutputStream(this.optimizationLog, true));
            }

            OptimizationRecord record = new OptimizationRecord();

            record.time = time;
            record.intFieldsMerged = intFields;
            record.stringFieldsMerged = stringFields;
            record.shardLocation = w.getOutputDirectory();
            record.mergedShards = new ArrayList<ShardMergeInfo>();

            ShardMergeInfo info = new ShardMergeInfo();
            info.numDocs = this.flamdexReader.getNumDocs();
            info.dynamicMetrics = this.getDynamicMetrics();
            info.newDocIdToOldDocId = rewriter.getPerSessionMappings().get(0);
            record.mergedShards.add(info);

            oos.writeObject(record);
            oos.close();

            /* use rebuilt structures */
            memory.releaseMemory(this.docIdToGroup.memoryUsed());
            rewriterMemory.hoist(rewriter.getNewGroupLookup().memoryUsed());
            this.docIdToGroup = rewriter.getNewGroupLookup();

            for (DynamicMetric dm : this.dynamicMetrics.values()) {
                memory.releaseMemory(dm.memoryUsed());
            }
            for (DynamicMetric dm : rewriter.getDynamicMetrics().values()) {
                rewriterMemory.hoist(dm.memoryUsed());
            }
            this.dynamicMetrics = rewriter.getDynamicMetrics();

            /* release memory used by the index rewriter */
            rewriterMemory.close();

            /*
             * replace flamdexReader pointers, but keep the originals in case
             * there is a reset() call
             */
            if (this.originalReader == null) {
                this.originalReader = this.flamdexReader;
            }
            if (this.originalReaderRef == null) {
                this.originalReaderRef = this.flamdexReaderRef;
            } else {
                /* close the unnecessary optimized index */
                this.flamdexReaderRef.close();
            }

            FlamdexReader flamdex = AutoDeletingReader.open(w.getOutputDirectory());
            if (flamdex instanceof RawFlamdexReader) {
                this.flamdexReader =
                        new RawCachedFlamdexReader(new MemoryReservationContext(memory),
                                                   (RawFlamdexReader) flamdex, null, null, null,
                                                   null);
            } else {
                this.flamdexReader =
                        new CachedFlamdexReader(new MemoryReservationContext(memory), flamdex,
                                                null, null, null, null);
            }
            this.flamdexReaderRef = SharedReference.create(this.flamdexReader);

            /* alter tracking fields to reflect the removal of group 0 docs */
            this.numDocs = this.flamdexReader.getNumDocs();
            this.groupDocCount[0] = 0;

            /* push the stats back on */
            for (String stat : statsCopy) {
                if ("pop".equals(stat)) {
                    this.popStat();
                } else {
                    this.pushStat(stat);
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /*
     * Resets the Falmdex readers to the original un-optimized versions and
     * constructs the DynamicMetrics to match what they should be if no
     * optimization had taken place.
     * 
     * The GroupLookup does not have to be reconstructed since it will be set to
     * a constant value as a result of a reset() call
     */
    private synchronized void resetOptimizedReaders() throws ImhotepOutOfMemoryException {
        ObjectInputStream ois = null;
        ArrayList<OptimizationRecord> records = new ArrayList<OptimizationRecord>();
        final long memoryUse;
        final ArrayList<String> statsCopy;

        /* check if this session has been optimized */
        if (this.originalReader == null) {
            return;
        }

        /* check for space in memory */
        memoryUse = this.optimizationLog.length();
        if (!this.memory.claimMemory(memoryUse)) {
            throw new ImhotepOutOfMemoryException();
        }

        /* pop off all the stats, they will be repushed after the flamdex reset */
        statsCopy = new ArrayList<String>(this.statCommands);
        while (this.numStats > 0) {
            this.popStat();
        }
        this.statCommands.clear();

        /* read in all the optimization records */
        try {
            ois = new ObjectInputStream(new FileInputStream(this.optimizationLog));
            while (true) {
                /*
                 * adds the records so the last written record is first in the
                 * list
                 */
                records.add(0, (OptimizationRecord) ois.readObject());
            }
        } catch (EOFException e) {
            // read all the records
            try {
                if (ois != null) {
                    ois.close();
                    this.optimizationLog.delete();
                }
            } catch (IOException e1) {
                /* do nothing */
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            /* the log is no longer needed, so remove it */
            this.optimizationLog.delete();
        }

        /* reconstruct the dynamic metrics */
        Map<String, DynamicMetric> newMetrics;
        Map<String, DynamicMetric> oldMetrics;
        int numNewDocs;
        int numOldDocs;
        newMetrics = this.dynamicMetrics;
        numNewDocs = this.flamdexReader.getNumDocs();
        for (OptimizationRecord opRec : records) {
            int[] newToOldIdMapping = opRec.mergedShards.get(0).newDocIdToOldDocId;
            oldMetrics = opRec.mergedShards.get(0).dynamicMetrics;
            numOldDocs = opRec.mergedShards.get(0).numDocs;

            for (Map.Entry<String, DynamicMetric> e : newMetrics.entrySet()) {
                DynamicMetric oldMetric = oldMetrics.get(e.getKey());
                DynamicMetric newMetric = e.getValue();
                if (oldMetric == null) {
                    oldMetric = new DynamicMetric(numOldDocs);
                }
                for (int i = 0; i < numNewDocs; i++) {
                    int oldId = newToOldIdMapping[i];
                    int value = newMetric.lookupSingleVal(i);
                    oldMetric.set(oldId, value);
                }

                oldMetrics.put(e.getKey(), oldMetric);
            }
            numNewDocs = numOldDocs;
            newMetrics = oldMetrics;
        }

        /* adjust the memory tracking */
        for (DynamicMetric dm : this.dynamicMetrics.values()) {
            memory.releaseMemory(dm.memoryUsed());
        }
        for (DynamicMetric dm : newMetrics.values()) {
            memory.claimMemory(dm.memoryUsed());
        }
        this.dynamicMetrics = newMetrics;

        try {
            /* close temp index reader */
            this.flamdexReaderRef.close();
        } catch (IOException e) {
            log.error("Could not close optimized reader");
        }

        /* reopen the original flamdex readers */
        this.flamdexReader = this.originalReader;
        this.flamdexReaderRef = this.originalReaderRef;
        this.originalReader = null;
        this.originalReaderRef = null;

        this.numDocs = this.flamdexReader.getNumDocs();

        /* push the stats back on */
        for (String stat : statsCopy) {
            if ("pop".equals(stat)) {
                this.popStat();
            } else {
                this.pushStat(stat);
            }
        }

        /* release the memory used by the log reading */
        this.memory.releaseMemory(memoryUse);

    }

    /**
     * export the current docId -> group lookup into an array
     *
     * @param array
     *            the array to export docIdToGroup into
     */
    public synchronized void exportDocIdToGroupId(int[] array) {
        if (array.length != docIdToGroup.size()) {
            throw new IllegalArgumentException("array length is invalid");
        }
        for (int i = array.length - 1; i >= 0; --i) {
            array[i] = docIdToGroup.get(i);
        }
    }

    @Override
    public synchronized long getTotalDocFreq(String[] intFields, String[] stringFields) {
        long ret = 0L;

        for (final String intField : intFields) {
            ret += flamdexReader.getIntTotalDocFreq(intField);
        }

        for (final String stringField : stringFields) {
            ret += flamdexReader.getStringTotalDocFreq(stringField);
        }

        return ret;
    }

    @Override
    public synchronized FTGSIterator getFTGSIterator(String[] intFields, String[] stringFields) {
        if (fieldZeroDocBitsets != null) {
            for (String intField : intFields) {
                if (!fieldZeroDocBitsets.containsKey(Pair.of(intField, true))) {
                    if (memory.claimMemory(FastBitSet.calculateMemoryUsage(32))) {
                        fieldZeroDocBitsets.put(Pair.of(intField, true), new FastBitSet(32));
                    } else {
                        log.warn("Insufficient memory, not allocating ftgs group zero bitset for field "
                                + intField);
                    }
                }
            }
            for (String stringField : stringFields) {
                if (!fieldZeroDocBitsets.containsKey(Pair.of(stringField, true))) {
                    if (memory.claimMemory(FastBitSet.calculateMemoryUsage(32))) {
                        fieldZeroDocBitsets.put(Pair.of(stringField, false), new FastBitSet(32));
                    } else {
                        log.warn("Insufficient memory, not allocating ftgs group zero bitset for field "
                                + stringField);
                    }
                }
            }
        }
        if (flamdexReader instanceof RawFlamdexReader) {
            return new RawFlamdexFTGSIterator(this, flamdexReaderRef.copy(), intFields,
                                              stringFields);
        }
        return new FlamdexFTGSIterator(this, flamdexReaderRef.copy(), intFields, stringFields);
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(Map<String, long[]> intFields, Map<String, String[]> stringFields) {
        if (flamdexReader instanceof RawFlamdexReader) {
            return new RawFlamdexSubsetFTGSIterator(this, flamdexReaderRef.copy(), intFields,
                    stringFields);
        }
        return new FlamdexSubsetFTGSIterator(this, flamdexReaderRef.copy(), intFields, stringFields);
    }

    public DocIterator getDocIterator(final String[] intFields, final String[] stringFields) throws ImhotepOutOfMemoryException {
        boolean shardOnlyContainsGroupZero = true;
        for (int group = 1; group < groupDocCount.length; group++) {
            if (groupDocCount[group] != 0) {
                shardOnlyContainsGroupZero = false;
                break;
            }
        }
        if (shardOnlyContainsGroupZero) {
            return emptyDocIterator();
        }

        final IntValueLookup[] intValueLookups = new IntValueLookup[intFields.length];
        final StringValueLookup[] stringValueLookups = new StringValueLookup[stringFields.length];
        try {
            for (int i = 0; i < intFields.length; i++) {
                intValueLookups[i] = flamdexReader.getMetric(intFields[i]);
            }
            for (int i = 0; i < stringFields.length; i++) {
                stringValueLookups[i] = flamdexReader.getStringLookup(stringFields[i]);
            }
        } catch (FlamdexOutOfMemoryException e) {
            for (IntValueLookup lookup : intValueLookups) {
                if (lookup != null) {
                    lookup.close();
                }
            }
            for (StringValueLookup lookup : stringValueLookups) {
                if (lookup != null) {
                    lookup.close();
                }
            }
            throw new ImhotepOutOfMemoryException();
        }
        return new DocIterator() {

            int[] groups = new int[1024];
            int n = groups.length;
            int bufferStart = -groups.length;
            int docId = -1;
            boolean done = false;

            public boolean next() {
                if (done) {
                    return false;
                }
                while (true) {
                    docId++;
                    if (docId - bufferStart >= n) {
                        if (!readGroups()) {
                            return endOfData();
                        }
                    }
                    if (groups[docId - bufferStart] != 0) {
                        return true;
                    }
                }
            }

            boolean endOfData() {
                done = true;
                return false;
            }

            public boolean readGroups() {
                bufferStart += n;
                n = Math.min(numDocs - bufferStart, groups.length);
                if (n <= 0) {
                    return false;
                }
                docIdToGroup.fillDocGrpBufferSequential(bufferStart, groups, n);
                return true;
            }

            public int getGroup() {
                return groups[docId - bufferStart];
            }

            int[] docIdRef = new int[1];
            long[] valueRef = new long[1];

            public long getInt(final int index) {
                docIdRef[0] = docId;
                intValueLookups[index].lookup(docIdRef, valueRef, 1);
                return valueRef[0];
            }

            public String getString(final int index) {
                return stringValueLookups[index].getString(docId);
            }

            public void close() throws IOException {
                for (IntValueLookup lookup : intValueLookups) {
                    if (lookup != null) {
                        lookup.close();
                    }
                }
                for (StringValueLookup lookup : stringValueLookups) {
                    if (lookup != null) {
                        lookup.close();
                    }
                }
            }
        };
    }

    private static DocIterator emptyDocIterator() {
        return new DocIterator() {
            @Override
            public boolean next() {
                return false;
            }

            @Override
            public int getGroup() {
                return 0;
            }

            @Override
            public long getInt(int index) {
                return 0;
            }

            @Override
            public String getString(int index) {
                return null;
            }

            @Override
            public void close() {

            }
        };
    }

    public RawFTGSIterator[] getFTGSIteratorSplits(final String[] intFields, final String[] stringFields) {
        final int numSplits = 16;

        final RawFTGSIterator[] ret = new RawFTGSIterator[numSplits];
        for (int i = 0; i < numSplits; i++) {
            ret[i] = getFTGSIteratorSplit(intFields, stringFields, i, numSplits);
        }

        return ret;
    }

    public synchronized RawFTGSIterator getFTGSIteratorSplit(final String[] intFields,
                                                             final String[] stringFields,
                                                             final int splitIndex,
                                                             final int numSplits) {
        if (ftgsIteratorSplits == null || ftgsIteratorSplits.isClosed()) {
            try {
                ftgsIteratorSplits = new FTGSSplitter(getFTGSIterator(intFields, stringFields), numSplits, numStats, "getIteratorSplitsLocalSession", 969168349);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        return ftgsIteratorSplits.getFtgsIterators()[splitIndex];
    }

    @Override
    public synchronized RawFTGSIterator getSubsetFTGSIteratorSplit(Map<String, long[]> intFields, Map<String, String[]> stringFields, int splitIndex, int numSplits) {
        if (ftgsIteratorSplits == null || ftgsIteratorSplits.isClosed()) {
            try {
                ftgsIteratorSplits = new FTGSSplitter(getSubsetFTGSIterator(intFields, stringFields), numSplits, numStats, "getIteratorSplitsLocalSession", 969168349);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
        return ftgsIteratorSplits.getFtgsIterators()[splitIndex];
    }

    public RawFTGSIterator mergeFTGSSplit(final String[] intFields,
                                          final String[] stringFields,
                                          final String sessionId,
                                          final InetSocketAddress[] nodes,
                                          final int splitIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RawFTGSIterator mergeSubsetFTGSSplit(Map<String, long[]> intFields, Map<String, String[]> stringFields, String sessionId, InetSocketAddress[] nodes, int splitIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized int regroup(final GroupMultiRemapRule[] rules, boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final int numRules = rules.length;
        if (numRules == 0) {
            resetGroupsTo(0);
            return docIdToGroup.getNumGroups();
        }

        final int numConditions = MultiRegroupInternals.countRemapConditions(rules);
        final int highestTarget;
        final int targetGroupBytes = Math.max(numRules * 4, numConditions * 8);
        if (!memory.claimMemory(targetGroupBytes)) {
            throw new ImhotepOutOfMemoryException();
        }
        try {
            highestTarget = MultiRegroupInternals.validateTargets(rules);
            MultiRegroupInternals.validateEqualitySplits(rules);
        } finally {
            memory.releaseMemory(targetGroupBytes);
        }

        final int maxIntermediateGroup = Math.max(docIdToGroup.getNumGroups(), highestTarget);
        final int maxNewGroup = MultiRegroupInternals.findMaxGroup(rules);
        docIdToGroup =
                GroupLookupFactory.resize(docIdToGroup,
                                          Math.max(maxIntermediateGroup, maxNewGroup),
                                          memory);
        MultiRegroupInternals.moveUntargeted(docIdToGroup, maxIntermediateGroup, rules);

        final int maxConditionIndex = MultiRegroupInternals.findMaxIntermediateGroup(rules);
        final int placeholderGroup = maxConditionIndex + 1;

        final int parallelArrayBytes = 3 * 4 * numConditions + 8 * numConditions;
        // int[highestTarget+1], int[highestTarget+1][], <int or
        // string>[highestTarget+1][]
        // The last two are jagged arrays, and the cardinality of the subarrays
        // sums to numConditions at most
        final int maxInequalityBytes = (highestTarget + 1) * (4 + 8 + 8) + numConditions * (4 + 8);
        final int maxBarrierIndexBytes = numConditions * 4;
        final int remappingBytes = (maxIntermediateGroup + 1) * 4;
        final int totalInternalRegroupBytes =
                parallelArrayBytes + maxInequalityBytes + maxBarrierIndexBytes + remappingBytes;
        final GroupLookup newDocIdToGroup;
        newDocIdToGroup = newGroupLookupWithPlaceholders(placeholderGroup);

        try {
            if (!memory.claimMemory(totalInternalRegroupBytes)) {
                throw new ImhotepOutOfMemoryException();
            }
            try {
                MultiRegroupInternals.internalMultiRegroup(docIdToGroup,
                                                           newDocIdToGroup,
                                                           docIdBuf,
                                                           flamdexReader,
                                                           rules,
                                                           highestTarget,
                                                           numConditions,
                                                           placeholderGroup,
                                                           maxIntermediateGroup,
                                                           errorOnCollisions);
            } finally {
                memory.releaseMemory(totalInternalRegroupBytes);
            }

            final int targetGroupToRuleBytes =
                    Math.max(highestTarget + 1, docIdToGroup.getNumGroups()) * 8;
            if (!memory.claimMemory(targetGroupToRuleBytes)) {
                throw new ImhotepOutOfMemoryException();
            }
            try {
                MultiRegroupInternals.internalMultiRegroupCleanup(docIdToGroup,
                                                                  docIdToGroup.getNumGroups(),
                                                                  rules,
                                                                  highestTarget,
                                                                  newDocIdToGroup,
                                                                  placeholderGroup);
            } finally {
                memory.releaseMemory(targetGroupToRuleBytes);
            }
        } finally {
            memory.releaseMemory(newDocIdToGroup.memoryUsed());
        }

        finalizeRegroup();

        return docIdToGroup.getNumGroups();
    }

    // Makes a new GroupLookup with all documents having a nonzero group in the
    // current docIdToGroup
    // having a group of placeholderGroup.
    private synchronized GroupLookup newGroupLookupWithPlaceholders(int placeholderGroup) throws ImhotepOutOfMemoryException {
        final GroupLookup newLookup;

        newLookup = GroupLookupFactory.create(placeholderGroup, docIdToGroup.size(), this, memory);

        for (int i = 0; i < newLookup.size(); i++) {
            if (docIdToGroup.get(i) != 0) {
                newLookup.set(i, placeholderGroup);
            }
        }
        return newLookup;
    }

    @Override
    public synchronized int regroup(final GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        for (GroupRemapRule rule : rawRules) {
            if (rule.targetGroup == 0) {
                clearZeroDocBitsets();
            }
        }
        final int requiredMemory = numDocs / 8 + 1;
        if (!memory.claimMemory(requiredMemory)) {
            throw new ImhotepOutOfMemoryException();
        }
        try {
            internalRegroup(rawRules);
        } finally {
            memory.releaseMemory(requiredMemory);
        }
        return docIdToGroup.getNumGroups();
    }

    private void ensureGroupLookupCapacity(GroupRemapRule[] cleanRules) throws ImhotepOutOfMemoryException {
        int maxGroup = 0;

        for (final GroupRemapRule rule : cleanRules) {
            if (rule != null) {
                maxGroup = Math.max(maxGroup, Math.max(rule.negativeGroup, rule.positiveGroup));
            }
        }
        docIdToGroup = GroupLookupFactory.resize(docIdToGroup, maxGroup, memory);
    }

    private void internalRegroup(GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        final GroupRemapRule[] cleanRules = cleanUpRules(rawRules, docIdToGroup.getNumGroups());

        ensureGroupLookupCapacity(cleanRules);
        final ThreadSafeBitSet docRemapped = new ThreadSafeBitSet(numDocs);
        final DocIdStream docIdStream = flamdexReader.getDocIdStream();

        applyIntConditions(cleanRules, docIdStream, docRemapped);
        applyStringConditions(cleanRules, docIdStream, docRemapped);
        docIdStream.close();

        // pick up everything else that was missed
        for (int i = 0; i < docIdToGroup.size(); i++) {
            if (docRemapped.get(i)) {
                continue;
            }
            final int group = docIdToGroup.get(i);
            final int newGroup;
            if (cleanRules[group] != null) {
                newGroup = cleanRules[group].negativeGroup;
            } else {
                newGroup = 0;
            }
            docIdToGroup.set(i, newGroup);
        }

        finalizeRegroup();
    }

    private void finalizeRegroup() throws ImhotepOutOfMemoryException {
        final int oldNumGroups = docIdToGroup.getNumGroups();
        final int newNumGroups;

        docIdToGroup.recalculateNumGroups();
        newNumGroups = docIdToGroup.getNumGroups();
        accountForFlamdexFTGSIteratorMemChange(oldNumGroups, newNumGroups);
        docIdToGroup = GroupLookupFactory.resize(docIdToGroup, 0, memory);
        recalcGroupCounts(newNumGroups);
        recalcGroupStats(newNumGroups);
    }

    private void accountForFlamdexFTGSIteratorMemChange(final int oldNumGroups,
                                                        final int newNumGroups) throws ImhotepOutOfMemoryException {
        if (newNumGroups > oldNumGroups) {
            // for memory in FlamdexFTGSIterator
            if (!memory.claimMemory((12L + 8L * numStats) * (newNumGroups - oldNumGroups))) {
                throw new ImhotepOutOfMemoryException();
            }
        } else if (newNumGroups < oldNumGroups) {
            // for memory in FlamdexFTGSIterator
            memory.releaseMemory((12L + 8L * numStats) * (oldNumGroups - newNumGroups));
        }
    }

    @Override
    public int regroup(QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        if (rule.getTargetGroup() == 0) {
            clearZeroDocBitsets();
        }
        docIdToGroup =
                GroupLookupFactory.resize(docIdToGroup, Math.max(rule.getNegativeGroup(),
                                                                 rule.getPositiveGroup()), memory);

        final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
        final FastBitSet bitSet;
        try {
            bitSet = bitSetPooler.create(flamdexReader.getNumDocs());
        } catch (FlamdexOutOfMemoryException e) {
            throw new ImhotepOutOfMemoryException(e);
        }

        try {
            final FlamdexSearcher searcher = new FlamdexSearcher(flamdexReader);
            final Query query = rule.getQuery();
            searcher.search(query, bitSet, bitSetPooler);
            docIdToGroup.bitSetRegroup(bitSet,
                                       rule.getTargetGroup(),
                                       rule.getNegativeGroup(),
                                       rule.getPositiveGroup());
        } catch (FlamdexOutOfMemoryException e) {
            throw new ImhotepOutOfMemoryException(e);
        } finally {
            bitSetPooler.release(bitSet.memoryUsage());
        }

        finalizeRegroup();

        return docIdToGroup.getNumGroups();
    }

    @Override
    public synchronized void intOrRegroup(String field,
                                          long[] terms,
                                          int targetGroup,
                                          int negativeGroup,
                                          int positiveGroup) throws ImhotepOutOfMemoryException {
        if (targetGroup == 0) {
            clearZeroDocBitsets();
        }
        docIdToGroup =
                GroupLookupFactory.resize(docIdToGroup,
                                          Math.max(negativeGroup, positiveGroup),
                                          memory);

        final IntTermIterator iter = flamdexReader.getIntTermIterator(field);
        final DocIdStream docIdStream = flamdexReader.getDocIdStream();
        final ThreadSafeBitSet docRemapped = new ThreadSafeBitSet(numDocs); // necessary
                                                                            // in
                                                                            // case
                                                                            // targetGroup
                                                                            // ==
                                                                            // positiveGroup

        int termsIndex = 0;
        while (iter.next()) {
            final long term = iter.term();
            while (termsIndex < terms.length && terms[termsIndex] < term) {
                ++termsIndex;
            }

            if (termsIndex < terms.length && term == terms[termsIndex]) {
                docIdStream.reset(iter);
                remapPositiveDocs(docIdStream, docRemapped, targetGroup, positiveGroup);
                ++termsIndex;
            }

            if (termsIndex == terms.length) {
                break;
            }
        }
        docIdStream.close();
        iter.close();

        remapNegativeDocs(docRemapped, targetGroup, negativeGroup);

        finalizeRegroup();
    }

    @Override
    public synchronized void stringOrRegroup(String field,
                                             String[] terms,
                                             int targetGroup,
                                             int negativeGroup,
                                             int positiveGroup) throws ImhotepOutOfMemoryException {
        if (targetGroup == 0) {
            clearZeroDocBitsets();
        }
        docIdToGroup =
                GroupLookupFactory.resize(docIdToGroup,
                                          Math.max(negativeGroup, positiveGroup),
                                          memory);

        final StringTermIterator iter = flamdexReader.getStringTermIterator(field);
        final DocIdStream docIdStream = flamdexReader.getDocIdStream();
        final ThreadSafeBitSet docRemapped = new ThreadSafeBitSet(numDocs);

        int termsIndex = 0;
        while (iter.next()) {
            final String term = iter.term();
            while (termsIndex < terms.length && terms[termsIndex].compareTo(term) < 0) {
                ++termsIndex;
            }

            if (termsIndex < terms.length && terms[termsIndex].equals(term)) {
                docIdStream.reset(iter);
                remapPositiveDocs(docIdStream, docRemapped, targetGroup, positiveGroup);
                ++termsIndex;
            }

            if (termsIndex == terms.length) {
                break;
            }
        }
        docIdStream.close();
        iter.close();

        remapNegativeDocs(docRemapped, targetGroup, negativeGroup);

        finalizeRegroup();
    }

    private void remapNegativeDocs(ThreadSafeBitSet docRemapped, int targetGroup, int negativeGroup) {
        for (int doc = 0; doc < numDocs; ++doc) {
            if (!docRemapped.get(doc) && docIdToGroup.get(doc) == targetGroup) {
                docIdToGroup.set(doc, negativeGroup);
            }
        }
    }

    private void remapPositiveDocs(DocIdStream docIdStream,
                                   ThreadSafeBitSet docRemapped,
                                   int targetGroup,
                                   int positiveGroup) {
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            for (int i = 0; i < n; ++i) {
                final int doc = docIdBuf[i];
                if (docIdToGroup.get(doc) == targetGroup) {
                    docIdToGroup.set(doc, positiveGroup);
                    docRemapped.set(doc);
                }
            }
            if (n < docIdBuf.length) {
                break;
            }
        }
    }

    @Override
    public synchronized void randomRegroup(String field,
                                           boolean isIntField,
                                           String salt,
                                           double p,
                                           int targetGroup,
                                           int negativeGroup,
                                           int positiveGroup) throws ImhotepOutOfMemoryException {
        if (targetGroup == 0) {
            clearZeroDocBitsets();
        }
        docIdToGroup =
                GroupLookupFactory.resize(docIdToGroup,
                                          Math.max(negativeGroup, positiveGroup),
                                          memory);

        final ImhotepChooser chooser = new ImhotepChooser(salt, p);
        final DocIdStream docIdStream = flamdexReader.getDocIdStream();
        if (isIntField) {
            final IntTermIterator iter = flamdexReader.getIntTermIterator(field);
            while (iter.next()) {
                final long term = iter.term();
                final int newGroup =
                        chooser.choose(Long.toString(term)) ? positiveGroup : negativeGroup;
                docIdStream.reset(iter);
                while (true) {
                    final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        final int doc = docIdBuf[i];
                        if (docIdToGroup.get(doc) == targetGroup) {
                            docIdToGroup.set(doc, newGroup);
                        }
                    }

                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            iter.close();
        } else {
            final StringTermIterator iter = flamdexReader.getStringTermIterator(field);
            while (iter.next()) {
                final String term = iter.term();
                final int newGroup = chooser.choose(term) ? positiveGroup : negativeGroup;
                docIdStream.reset(iter);
                while (true) {
                    final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        final int doc = docIdBuf[i];
                        if (docIdToGroup.get(doc) == targetGroup) {
                            docIdToGroup.set(doc, newGroup);
                        }
                    }

                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            iter.close();
        }
        docIdStream.close();

        finalizeRegroup();
    }

    @Override
    public synchronized void randomMultiRegroup(String field,
                                                boolean isIntField,
                                                String salt,
                                                int targetGroup,
                                                double[] percentages,
                                                int[] resultGroups) throws ImhotepOutOfMemoryException {
        if (targetGroup == 0) {
            clearZeroDocBitsets();
        }
        ensureValidMultiRegroupArrays(percentages, resultGroups);
        docIdToGroup = GroupLookupFactory.resize(docIdToGroup, Ints.max(resultGroups), memory);

        final ImhotepChooser chooser = new ImhotepChooser(salt, -1.0); // We're
                                                                       // not
                                                                       // using
                                                                       // the
                                                                       // chooser's
                                                                       // percentage
        final DocIdStream docIdStream = flamdexReader.getDocIdStream();
        if (isIntField) {
            final IntTermIterator iter = flamdexReader.getIntTermIterator(field);
            while (iter.next()) {
                final long term = iter.term();
                final int groupIndex =
                        indexOfFirstLessThan(chooser.getValue(Long.toString(term)), percentages);
                final int newGroup = resultGroups[groupIndex];
                docIdStream.reset(iter);
                while (true) {
                    final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        final int doc = docIdBuf[i];
                        if (docIdToGroup.get(doc) == targetGroup) {
                            docIdToGroup.set(doc, newGroup);
                        }
                    }

                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            iter.close();
        } else {
            final StringTermIterator iter = flamdexReader.getStringTermIterator(field);
            while (iter.next()) {
                final String term = iter.term();
                final int groupIndex = indexOfFirstLessThan(chooser.getValue(term), percentages);
                final int newGroup = resultGroups[groupIndex];
                docIdStream.reset(iter);
                while (true) {
                    final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        final int doc = docIdBuf[i];
                        if (docIdToGroup.get(doc) == targetGroup) {
                            docIdToGroup.set(doc, newGroup);
                        }
                    }

                    if (n < docIdBuf.length) {
                        break;
                    }
                }
            }
            iter.close();
        }
        docIdStream.close();

        finalizeRegroup();
    }

    @Override
    public List<TermCount> approximateTopTerms(String field, boolean isIntField, int k) {
        k = Math.min(k, 1000);

        if (isIntField) {
            final PriorityQueue<IntTermWithFreq> pq =
                    new ObjectHeapPriorityQueue<IntTermWithFreq>(k, INT_FREQ_COMPARATOR);
            final IntTermIterator iter = flamdexReader.getIntTermIterator(field);
            try {
                while (iter.next()) {
                    final int docFreq = iter.docFreq();
                    if (pq.size() < k) {
                        pq.enqueue(new IntTermWithFreq(iter.term(), docFreq));
                    } else {
                        final IntTermWithFreq min = pq.first();
                        if (docFreq > min.docFreq) {
                            min.term = iter.term();
                            min.docFreq = docFreq;
                            pq.changed();
                        }
                    }
                }
                final List<TermCount> ret = Lists.newArrayListWithCapacity(pq.size());
                while (!pq.isEmpty()) {
                    final IntTermWithFreq term = pq.dequeue();
                    ret.add(new TermCount(new Term(field, true, term.term, ""), term.docFreq));
                }
                Collections.reverse(ret);
                return ret;
            } finally {
                iter.close();
            }
        } else {
            final PriorityQueue<StringTermWithFreq> pq =
                    new ObjectHeapPriorityQueue<StringTermWithFreq>(k, STRING_FREQ_COMPARATOR);
            final StringTermIterator iter = flamdexReader.getStringTermIterator(field);
            try {
                while (iter.next()) {
                    final int docFreq = iter.docFreq();
                    if (pq.size() < k) {
                        pq.enqueue(new StringTermWithFreq(iter.term(), docFreq));
                    } else {
                        final StringTermWithFreq min = pq.first();
                        if (docFreq > min.docFreq) {
                            min.term = iter.term();
                            min.docFreq = docFreq;
                            pq.changed();
                        }
                    }
                }
                final List<TermCount> ret = Lists.newArrayListWithCapacity(pq.size());
                while (!pq.isEmpty()) {
                    final StringTermWithFreq term = pq.dequeue();
                    ret.add(new TermCount(new Term(field, false, 0, term.term), term.docFreq));
                }
                Collections.reverse(ret);
                return ret;
            } finally {
                iter.close();
            }
        }
    }

    private static final Comparator<IntTermWithFreq> INT_FREQ_COMPARATOR =
            new Comparator<IntTermWithFreq>() {
                @Override
                public int compare(IntTermWithFreq o1, IntTermWithFreq o2) {
                    return Ints.compare(o1.docFreq, o2.docFreq);
                }
            };

    private static final class IntTermWithFreq {
        public long term;
        public int docFreq;

        private IntTermWithFreq(long term, int docFreq) {
            this.term = term;
            this.docFreq = docFreq;
        }
    }

    private static final Comparator<StringTermWithFreq> STRING_FREQ_COMPARATOR =
            new Comparator<StringTermWithFreq>() {
                @Override
                public int compare(StringTermWithFreq o1, StringTermWithFreq o2) {
                    return Ints.compare(o1.docFreq, o2.docFreq);
                }
            };

    private static final class StringTermWithFreq {
        public String term;
        public int docFreq;

        private StringTermWithFreq(String term, int docFreq) {
            this.term = term;
            this.docFreq = docFreq;
        }
    }

    /**
     * Requires that array is non-null and sorted in ascending order.
     *
     * Returns the lowest index in the array such that value < array[index]. If
     * value is greater than every element in array, returns array.length.
     * Essentially, a wrapper around binarySearch to return an index in all
     * cases.
     */
    protected int indexOfFirstLessThan(double value, double[] array) {
        int pos = Arrays.binarySearch(array, value);
        if (pos > 0) { // if pos > 0, then value == array[pos] --> continue
                       // until we find a greater element & break
            while (pos < array.length && array[pos] == value) {
                pos++;
            }
            return pos;
        } else {
            // when pos < 0, pos = (-(insertion point) - 1)
            return -(pos + 1);
        }
    }

    /**
     * Ensures that the percentages and resultGroups array are valid inputs for
     * a randomMultiRegroup. Otherwise, throws an IllegalArgumentException.
     * Specifically, checks to make sure
     * <ul>
     * <li>percentages is in ascending order,</li>
     * <li>percentages contains only values between 0.0 & 1.0, and</li>
     * <li>len(percentages) == len(resultGroups) - 1</li>
     * </ul>
     *
     * @see ImhotepLocalSession#randomMultiRegroup(String, boolean, String, int,
     *      double[], int[])
     */
    protected void ensureValidMultiRegroupArrays(double[] percentages, int[] resultGroups) throws IllegalArgumentException {
        // Ensure non-null inputs
        if (null == percentages || null == resultGroups) {
            throw new IllegalArgumentException(
                                               "received null percentages or resultGroups to randomMultiRegroup");
        }

        // Ensure that the lengths are correct
        if (percentages.length != resultGroups.length - 1) {
            throw new IllegalArgumentException(
                                               "percentages should have 1 fewer element than resultGroups");
        }

        // Ensure validity of percentages values
        double curr = 0.0;
        for (int i = 0; i < percentages.length; i++) {
            // Check: Increasing
            if (percentages[i] < curr) {
                throw new IllegalArgumentException("percentages values decreased between indices "
                        + (i - 1) + " and " + i);
            }

            // Check: between 0 and 1
            if (percentages[i] < 0.0 || percentages[i] > 1.0) {
                throw new IllegalArgumentException("percentages values should be between 0 and 1");
            }

            curr = percentages[i];
        }
    }

    @Override
    public synchronized int metricRegroup(int stat, long min, long max, long intervalSize, boolean noGutters) throws ImhotepOutOfMemoryException {
        clearZeroDocBitsets();
        if (stat < 0 || stat >= statLookup.length) {
            throw new IllegalArgumentException("invalid stat index: " + stat
                    + ", must be between [0," + statLookup.length + ")");
        }

        final int numBuckets = (int) (((max - 1) - min) / intervalSize + 1);
        final int newMaxGroup = (docIdToGroup.getNumGroups()-1)*(noGutters ? numBuckets : numBuckets+2);
        docIdToGroup = GroupLookupFactory.resize(docIdToGroup, newMaxGroup, memory);

        final IntValueLookup lookup = statLookup[stat];

        final int numDocs = docIdToGroup.size();
        for (int doc = 0; doc < numDocs; doc += BUFFER_SIZE) {

            final int n = Math.min(BUFFER_SIZE, numDocs - doc);

            docIdToGroup.fillDocGrpBufferSequential(doc, docGroupBuffer, n);

            int numNonZero = 0;
            for (int i = 0; i < n; ++i) {
                if (docGroupBuffer[i] != 0) {
                    docGroupBuffer[numNonZero] = docGroupBuffer[i];
                    docIdBuf[numNonZero++] = doc + i;
                }
            }

            if (numNonZero == 0) {
                continue;
            }

            lookup.lookup(docIdBuf, valBuf, numNonZero);

            if (noGutters) {
                internalMetricRegroupNoGutters(min, max, intervalSize, numBuckets, numNonZero);
            } else {
                internalMetricRegroupGutters(min, max, intervalSize, numBuckets, numNonZero);
            }

            docIdToGroup.batchSet(docIdBuf, docGroupBuffer, numNonZero);
        }

        finalizeRegroup();

        return docIdToGroup.getNumGroups();
    }

    private void internalMetricRegroupGutters(long min, long max, long intervalSize, int numBuckets, int numNonZero) {
        for (int i = 0; i < numNonZero; ++i) {
            final int group;
            final long val = valBuf[i];
            if (val < min) {
                group = numBuckets + 1;
            } else if (val >= max) {
                group = numBuckets + 2;
            } else {
                group = (int) ((val - min) / intervalSize + 1);
            }
            docGroupBuffer[i] = (docGroupBuffer[i] - 1) * (numBuckets + 2) + group;
        }
    }

    private void internalMetricRegroupNoGutters(long min, long max, long intervalSize, int numBuckets, int numNonZero) {
        for (int i = 0; i < numNonZero; ++i) {
            final long val = valBuf[i];
            if (val < min) {
                docGroupBuffer[i] = 0;
            } else if (val >= max) {
                docGroupBuffer[i] = 0;
            } else {
                final int group = (int) ((val - min) / intervalSize + 1);
                docGroupBuffer[i] = (docGroupBuffer[i]-1)*numBuckets+group;
            }
        }
    }

    @Override
    public synchronized int metricRegroup2D(int xStat,
                                            long xMin,
                                            long xMax,
                                            long xIntervalSize,
                                            int yStat,
                                            long yMin,
                                            long yMax,
                                            long yIntervalSize) throws ImhotepOutOfMemoryException {
        clearZeroDocBitsets();
        final int xBuckets = (int) (((xMax - 1) - xMin) / xIntervalSize + 3);
        final int yBuckets = (int) (((yMax - 1) - yMin) / yIntervalSize + 3);
        final int numGroups = xBuckets * yBuckets;

        docIdToGroup = GroupLookupFactory.resize(docIdToGroup, numGroups, memory);

        if (!memory.claimMemory(BUFFER_SIZE * 8)) {
            throw new ImhotepOutOfMemoryException();
        }
        try {
            final long[] yValBuf = new long[BUFFER_SIZE];

            final IntValueLookup xLookup = statLookup[xStat];
            final IntValueLookup yLookup = statLookup[yStat];

            final int numDocs = docIdToGroup.size();
            for (int doc = 0; doc < numDocs; doc += BUFFER_SIZE) {

                final int n = Math.min(BUFFER_SIZE, numDocs - doc);

                docIdToGroup.fillDocGrpBufferSequential(doc, docGroupBuffer, n);

                int numNonZero = 0;
                for (int i = 0; i < n; ++i) {
                    if (docGroupBuffer[i] != 0) {
                        docIdBuf[numNonZero++] = doc + i;
                    }
                }

                if (numNonZero == 0) {
                    continue;
                }

                xLookup.lookup(docIdBuf, valBuf, numNonZero);
                yLookup.lookup(docIdBuf, yValBuf, numNonZero);

                for (int i = 0; i < numNonZero; ++i) {
                    final long xVal = valBuf[i];
                    final long yVal = yValBuf[i];

                    final int group;
                    if (xVal < xMin) {
                        if (yVal < yMin) {
                            group = 1;
                        } else if (yVal >= yMax) {
                            group = (yBuckets - 1) * xBuckets + 1;
                        } else {
                            group = (int) (((yVal - yMin) / yIntervalSize + 1) * xBuckets + 1);
                        }
                    } else if (xVal >= xMax) {
                        if (yVal < yMin) {
                            group = xBuckets;
                        } else if (yVal >= yMax) {
                            group = xBuckets * yBuckets;
                        } else {
                            group = (int) (((yVal - yMin) / yIntervalSize + 2) * xBuckets);
                        }
                    } else {
                        if (yVal < yMin) {
                            group = (int) ((xVal - xMin) / xIntervalSize + 2);
                        } else if (yVal >= yMax) {
                            group =
                                    (int) ((yBuckets - 1) * xBuckets + (xVal - xMin)
                                            / xIntervalSize + 2);
                        } else {
                            group =
                                    (int) (((yVal - yMin) / yIntervalSize + 1) * xBuckets
                                            + (xVal - xMin) / xIntervalSize + 2);
                        }
                    }

                    docGroupBuffer[i] = group;
                }

                docIdToGroup.batchSet(docIdBuf, docGroupBuffer, numNonZero);
            }
        } finally {
            memory.releaseMemory(BUFFER_SIZE * 8);
        }

        finalizeRegroup();

        return numGroups;
    }

    public synchronized int metricFilter(int stat, long min, long max, final boolean negate) throws ImhotepOutOfMemoryException {
        clearZeroDocBitsets();
        if (stat < 0 || stat >= statLookup.length) {
            throw new IllegalArgumentException("invalid stat index: " + stat
                    + ", must be between [0," + statLookup.length + ")");
        }
        docIdToGroup = GroupLookupFactory.resize(docIdToGroup, docIdToGroup.getNumGroups(), memory);
        final IntValueLookup lookup = statLookup[stat];

        final int numDocs = docIdToGroup.size();
        for (int doc = 0; doc < numDocs; doc += BUFFER_SIZE) {

            final int n = Math.min(BUFFER_SIZE, numDocs - doc);

            docIdToGroup.fillDocGrpBufferSequential(doc, docGroupBuffer, n);

            int numNonZero = 0;
            for (int i = 0; i < n; ++i) {
                final int group = docGroupBuffer[i];
                if (group != 0) {
                    docIdBuf[numNonZero] = doc + i;
                    docGroupBuffer[numNonZero++] = group;
                }
            }

            if (numNonZero == 0) {
                continue;
            }

            lookup.lookup(docIdBuf, valBuf, numNonZero);

            for (int i = 0; i < numNonZero; ++i) {
                final long val = valBuf[i];
                final boolean valInRange = val >= min && val <= max;
                if (valInRange == negate) {
                    docGroupBuffer[i] = 0;
                }
            }

            docIdToGroup.batchSet(docIdBuf, docGroupBuffer, numNonZero);
        }

        finalizeRegroup();

        return docIdToGroup.getNumGroups();
    }

    @Override
    public synchronized long[] getGroupStats(int stat) {
        if (needToReCalcGroupStats[stat]) {
            updateGroupStatsAllDocs(statLookup[stat],
                                    groupStats[stat],
                                    docIdToGroup,
                                    docGroupBuffer,
                                    docIdBuf,
                                    valBuf);
            needToReCalcGroupStats[stat] = false;
        }
        return groupStats[stat];
    }

    private static GroupRemapRule[] cleanUpRules(GroupRemapRule[] rawRules, int numGroups) {
        final GroupRemapRule[] cleanRules = new GroupRemapRule[numGroups];
        for (final GroupRemapRule rawRule : rawRules) {
            if (rawRule.targetGroup >= cleanRules.length) {
                continue; // or error?
            }
            if (cleanRules[rawRule.targetGroup] != null) {
                continue; // or error?
            }
            cleanRules[rawRule.targetGroup] = rawRule;
        }
        return cleanRules;
    }

    private void recalcGroupStats(int numGroups) throws ImhotepOutOfMemoryException {
        for (int statIndex = 0; statIndex < numStats; statIndex++) {
            groupStats[statIndex] = clearAndResize(groupStats[statIndex], numGroups, memory);
            needToReCalcGroupStats[statIndex] = true;
        }
    }

    private void recalcGroupCounts(int numGroups) throws ImhotepOutOfMemoryException {
        groupDocCount = clearAndResize(groupDocCount, numGroups, memory);
        for (int i = 0; i < numDocs; i++) {
            groupDocCount[docIdToGroup.get(i)]++;
        }
    }

    private static final String decimalPattern = "-?[0-9]*\\.?[0-9]+";

    private static final Pattern floatScalePattern =
            Pattern.compile("floatscale\\s+(\\w+)\\s*\\*\\s*(" + decimalPattern + ")\\s*\\+\\s*("
                    + decimalPattern + ")");

    @Override
    public synchronized int pushStat(String statName) throws ImhotepOutOfMemoryException {
        if (numStats == MAX_NUMBER_STATS) {
            throw new IllegalArgumentException("Maximum number of stats exceeded");
        }

        if (statName.startsWith("hasstr ")) {
            final String s = statName.substring(7).trim();
            final String[] split = s.split(":", 2);
            if (split.length < 2) {
                throw new IllegalArgumentException("invalid hasstr metric: " + statName);
            }
            statLookup[numStats] = hasStringTermFilter(split[0], split[1]);
        } else if (statName.startsWith("hasint ")) {
            final String s = statName.substring(7).trim();
            final String[] split = s.split(":", 2);
            if (split.length < 2) {
                throw new IllegalArgumentException("invalid hasint metric: " + statName);
            }
            statLookup[numStats] = hasIntTermFilter(split[0], Integer.parseInt(split[1]));
        } else if (statName.startsWith("inttermcount ")) {
            final String field = statName.substring(13).trim();
            statLookup[numStats] = intTermCountLookup(field);
        } else if (statName.startsWith("strtermcount ")) {
            final String field = statName.substring(13).trim();
            statLookup[numStats] = stringTermCountLookup(field);
        } else if (statName.startsWith("floatscale ")) {
            final Matcher matcher = floatScalePattern.matcher(statName);
            // accepted format is 'floatscale field*scale+offset' (or just look
            // at the pattern)
            if (!matcher.matches()) {
                throw new IllegalArgumentException("invalid floatscale metric: " + statName);
            }

            final String field = matcher.group(1);
            final double scale;
            final double offset;
            try {
                scale = Double.parseDouble(matcher.group(2));
                offset = Double.parseDouble(matcher.group(3));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid offset or scale constant for metric: "
                        + statName, e);
            }

            statLookup[numStats] = scaledFloatLookup(field, scale, offset);
        } else if (statName.startsWith("dynamic ")) {
            final String name = statName.substring(8).trim();
            final DynamicMetric metric = getDynamicMetrics().get(name);
            if (metric == null) {
                throw new IllegalArgumentException("invalid dynamic metric: " + name);
            }
            statLookup[numStats] = metric;
        } else if (statName.startsWith("exp ")) {
            final int scaleFactor = Integer.valueOf(statName.substring(4).trim());
            final IntValueLookup operand = popLookup();
            statLookup[numStats] = new Exponential(operand, scaleFactor);
        } else if (statName.startsWith("log ")) {
            final int scaleFactor = Integer.valueOf(statName.substring(4).trim());
            final IntValueLookup operand = popLookup();
            statLookup[numStats] = new Log(operand, scaleFactor);
        } else if (statName.startsWith("ref ")) {
            final int depth = Integer.valueOf(statName.substring(4).trim());
            statLookup[numStats] = new DelegatingMetric(statLookup[numStats - depth - 1]);
        } else if (is32BitInteger(statName)) {
            final int constant = Integer.parseInt(statName); // guaranteed not
                                                             // to fail
            statLookup[numStats] = new Constant(constant);
        } else if (statName.startsWith("interleave ")) {
            final int count = Integer.valueOf(statName.substring(11).trim());

            final IntValueLookup[] originals = new IntValueLookup[count];
            final int start = numStats - count;
            if (start < 0) {
                throw new IllegalArgumentException(statName + ": expected at least " + count
                        + " metrics on stack, found " + numStats);
            }

            for (int i = 0; i < count; i++) {
                originals[i] = statLookup[start + i];
            }
            final IntValueLookup[] cached =
                    new CachedInterleavedMetrics(memory, flamdexReader.getNumDocs(), originals).getLookups();

            for (int i = 0; i < count; i++) {
                statLookup[start + i].close();
                statLookup[start + i] = cached[i];
            }

            /* this request is valid, so keep track of the command */
            this.statCommands.add(statName);

            return numStats; // cleanup below only applies if we're increasing
                             // the number of metrics
        } else if (statName.startsWith("mulshr ")) {
            final int shift = Integer.valueOf(statName.substring(7).trim());
            if (shift < 0 || shift > 31) {
                throw new IllegalArgumentException(
                                                   "mulshr shift value must be between 0 and 31 (inclusive)");
            }
            final IntValueLookup b = popLookup();
            final IntValueLookup a = popLookup();
            statLookup[numStats] = new MultiplyAndShiftRight(a, b, shift);
        } else if (statName.startsWith("shldiv ")) {
            final int shift = Integer.valueOf(statName.substring(7).trim());
            if (shift < 0 || shift > 31) {
                throw new IllegalArgumentException(
                                                   "shldiv shift value must be between 0 and 31 (inclusive)");
            }
            final IntValueLookup b = popLookup();
            final IntValueLookup a = popLookup();
            statLookup[numStats] = new ShiftLeftAndDivide(a, b, shift);
        } else if (statName.startsWith("log1pexp ")) {
            final int scale = Integer.valueOf(statName.substring(9).trim());
            final IntValueLookup operand = popLookup();
            statLookup[numStats] = new Log1pExp(operand, scale);
        } else if (statName.startsWith("logistic ")) {
            final String[] params = statName.substring(9).split(" ");
            if (params.length != 2) {
                throw new IllegalArgumentException("logistic requires 2 arguments: "+statName);
            }
            final double scaleDown;
            final double scaleUp;
            try {
                scaleDown = Double.parseDouble(params[0]);
                scaleUp = Double.parseDouble(params[1]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid scale factor for metric: "
                        + statName, e);
            }
            final IntValueLookup operand = popLookup();
            statLookup[numStats] = new Logistic(operand, scaleDown, scaleUp);
        } else if (statName.startsWith("lucene ")) {
            final String queryBase64 = statName.substring(7);
            final byte[] queryBytes = Base64.decodeBase64(queryBase64.getBytes());
            final QueryMessage queryMessage;
            try {
                queryMessage = QueryMessage.parseFrom(queryBytes);
            } catch (InvalidProtocolBufferException e) {
                throw Throwables.propagate(e);
            }
            final Query query = ImhotepDaemonMarshaller.marshal(queryMessage);

            final int bitSetMemory = (flamdexReader.getNumDocs() + 64) / 64 * 8;
            memory.claimMemory(bitSetMemory);
            try {
                final FastBitSet bitSet = new FastBitSet(flamdexReader.getNumDocs());
                final FastBitSetPooler bitSetPooler = new ImhotepBitSetPooler(memory);
                final FlamdexSearcher searcher = new FlamdexSearcher(flamdexReader);
                searcher.search(query, bitSet, bitSetPooler);
                statLookup[numStats] = new com.indeed.flamdex.fieldcache.BitSetIntValueLookup(bitSet);
            } catch (Throwable t) {
                memory.releaseMemory(bitSetMemory);
                if (t instanceof FlamdexOutOfMemoryException) throw new ImhotepOutOfMemoryException(t);
                throw Throwables2.propagate(t, ImhotepOutOfMemoryException.class);
            }
        } else if (Metric.getMetric(statName) != null) {
            final IntValueLookup a;
            final IntValueLookup b;
            switch (Metric.getMetric(statName)) {
            case COUNT:
                statLookup[numStats] = new Count();
                break;
            case CACHED:
                a = popLookup();
                try {
                    statLookup[numStats] = new CachedMetric(a, flamdexReader.getNumDocs(), memory);
                } finally {
                    a.close();
                }
                break;
            case ADD:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new Addition(a, b);
                break;
            case SUBTRACT:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new Subtraction(a, b);
                break;
            case MULTIPLY:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new Multiplication(a, b);
                break;
            case DIVIDE:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new Division(a, b);
                break;
            case MODULUS:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new Modulus(a, b);
                break;
            case ABSOLUTE_VALUE:
                a = popLookup();
                statLookup[numStats] = new AbsoluteValue(a);
                break;
            case MIN:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new Min(a, b);
                break;
            case MAX:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new Max(a, b);
                break;
            case EQ:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new Equal(a, b);
                break;
            case NE:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new NotEqual(a, b);
                break;
            case LT:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new LessThan(a, b);
                break;
            case LTE:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new LessThanOrEqual(a, b);
                break;
            case GT:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new GreaterThan(a, b);
                break;
            case GTE:
                b = popLookup();
                a = popLookup();
                statLookup[numStats] = new GreaterThanOrEqual(a, b);
                break;
            default:
                throw new RuntimeException("this is a bug");
            }
        } else {
            try {
                statLookup[numStats] = flamdexReader.getMetric(statName);
            } catch (FlamdexOutOfMemoryException e) {
                throw new ImhotepOutOfMemoryException(e);
            }
        }
        // todo: check if metric is invalid... ?
        groupStats[numStats] = clearAndResize((long[]) null, docIdToGroup.getNumGroups(), memory);
        needToReCalcGroupStats[numStats] = true;
        numStats++;

        // FlamdexFTGSIterator.termGrpStats
        if (!memory.claimMemory(8L * docIdToGroup.getNumGroups())) {
            throw new ImhotepOutOfMemoryException();
        }

        /* this request is valid, so keep track of the command */
        this.statCommands.add(statName);

        return numStats;
    }

    @Override
    public synchronized int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        for (String statName : statNames) {
            this.pushStat(statName);
        }

        return numStats;
    }

    private static boolean is32BitInteger(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private IntValueLookup popLookup() {
        if (numStats == 0) {
            throw new IllegalStateException("no stat to pop");
        }
        --numStats;

        final IntValueLookup ret = statLookup[numStats];
        statLookup[numStats] = null;
        final long memFreed = groupStats[numStats].length * 8 + 8L * docIdToGroup.getNumGroups();
        groupStats[numStats] = null;

        memory.releaseMemory(memFreed);

        return ret;
    }

    @Override
    public synchronized int popStat() {
        popLookup().close();

        /* this request is valid, so keep track of the command */
        this.statCommands.add("pop");

        return numStats;
    }

    @Override
    public synchronized int getNumStats() {
        return numStats;
    }


    @Override
    public long getLowerBound(int stat) {
        return statLookup[stat].getMin();
    }

    @Override
    public long getUpperBound(int stat) {
        return statLookup[stat].getMax();
    }

    @Override
    public synchronized void createDynamicMetric(String name) throws ImhotepOutOfMemoryException {
        if (getDynamicMetrics().containsKey(name)) {
            throw new RuntimeException("dynamic metric \"" + name + "\" already exists");
        }
        if (!memory.claimMemory(flamdexReader.getNumDocs() * 4L)) {
            throw new ImhotepOutOfMemoryException();
        }
        getDynamicMetrics().put(name, new DynamicMetric(flamdexReader.getNumDocs()));
    }

    @Override
    public synchronized void updateDynamicMetric(String name, int[] deltas) throws ImhotepOutOfMemoryException {
        final DynamicMetric metric = getDynamicMetrics().get(name);
        if (metric == null) {
            throw new RuntimeException("dynamic metric \"" + name + "\" does not exist");
        }

        final int numDocs = flamdexReader.getNumDocs();
        for (int doc = 0; doc < numDocs; doc++) {
            final int group = docIdToGroup.get(doc);
            if (group >= 0 && group < deltas.length) {
                metric.add(doc, deltas[group]);
            }
        }

        // pessimistically recompute all stats -- other metrics may indirectly
        // refer to this one
        for (int i = 0; i < numStats; i++) {
            needToReCalcGroupStats[i] = true;
            groupStats[i] = clearAndResize(groupStats[i], docIdToGroup.getNumGroups(), memory);
        }
    }

    @Override
    public synchronized void conditionalUpdateDynamicMetric(String name,
                                                            final RegroupCondition[] conditions,
                                                            final int[] deltas) {
        validateConditionalUpdateDynamicMetricInput(conditions, deltas);
        final DynamicMetric metric = getDynamicMetrics().get(name);
        if (metric == null) {
            throw new RuntimeException("dynamic metric \"" + name + "\" does not exist");
        }

        final List<Integer> indexes = Lists.newArrayList();
        for (int i = 0; i < conditions.length; i++) {
            indexes.add(i);
        }

        // I don't think it's really worth claiming memory for this, so I won't.
        final ImmutableListMultimap<Pair<String, Boolean>, Integer> fieldIndexMap =
                Multimaps.index(indexes, new Function<Integer, Pair<String, Boolean>>() {
                    @Override
                    public Pair<String, Boolean> apply(Integer index) {
                        return Pair.of(conditions[index].field, conditions[index].intType);
                    }
                });
        for (Pair<String, Boolean> field : fieldIndexMap.keySet()) {
            final String fieldName = field.getFirst();
            final boolean fieldIsIntType = field.getSecond();
            final List<Integer> indices = Lists.newArrayList(fieldIndexMap.get(field));
            // Sort within the field
            Collections.sort(indices, new Comparator<Integer>() {
                @Override
                public int compare(Integer o1, Integer o2) {
                    if (fieldIsIntType) {
                        return Longs.compare(conditions[o1].intTerm, conditions[o2].intTerm);
                    } else {
                        return conditions[o1].stringTerm.compareTo(conditions[o2].stringTerm);
                    }
                }
            });
            final DocIdStream docIdStream = flamdexReader.getDocIdStream();
            if (fieldIsIntType) {
                final IntTermIterator termIterator = flamdexReader.getIntTermIterator(fieldName);
                for (int index : indices) {
                    final long term = conditions[index].intTerm;
                    final int delta = deltas[index];
                    termIterator.reset(term);
                    if (termIterator.next() && termIterator.term() == term) {
                        docIdStream.reset(termIterator);
                        adjustDeltas(metric, docIdStream, delta);
                    }
                }
            } else {
                final StringTermIterator termIterator =
                        flamdexReader.getStringTermIterator(fieldName);
                for (int index : indices) {
                    final String term = conditions[index].stringTerm;
                    final int delta = deltas[index];
                    termIterator.reset(term);
                    if (termIterator.next() && termIterator.term().equals(term)) {
                        docIdStream.reset(termIterator);
                        adjustDeltas(metric, docIdStream, delta);
                    }
                }
            }
        }
    }

    private void validateConditionalUpdateDynamicMetricInput(RegroupCondition[] conditions,
                                                             int[] deltas) {
        if (conditions.length != deltas.length) {
            throw new IllegalArgumentException("conditions and deltas must be of the same length");
        }
        for (RegroupCondition condition : conditions) {
            if (condition.inequality) {
                throw new IllegalArgumentException(
                                                   "inequality conditions not currently supported by conditionalUpdateDynamicMetric!");
            }
        }
    }

    public void groupConditionalUpdateDynamicMetric(String name, int[] groups, RegroupCondition[] conditions, int[] deltas) {
        if (groups.length != conditions.length) {
            throw new IllegalArgumentException("groups and conditions must be the same length");
        }
        validateConditionalUpdateDynamicMetricInput(conditions, deltas);
        final DynamicMetric metric = getDynamicMetrics().get(name);
        if (metric == null) {
            throw new RuntimeException("dynamic metric \"" + name + "\" does not exist");
        }
        final IntArrayList groupsSet = new IntArrayList();
        final FastBitSet groupsWithCurrentTerm = new FastBitSet(docIdToGroup.getNumGroups());
        final int[] groupToDelta = new int[docIdToGroup.getNumGroups()];
        final Map<String, Long2ObjectMap<Pair<IntArrayList, IntArrayList>>> intFields = Maps.newHashMap();
        final Map<String, Map<String, Pair<IntArrayList, IntArrayList>>> stringFields = Maps.newHashMap();
        for (int i = 0; i < groups.length; i++) {
            //if the last group(s) exist on other shards but not this one docIdToGroup.getNumGroups() is wrong
            if (groups[i] >= groupToDelta.length) continue;
            final RegroupCondition condition = conditions[i];
            Pair<IntArrayList, IntArrayList> groupDeltas;
            if (condition.intType) {
                Long2ObjectMap<Pair<IntArrayList, IntArrayList>> termToGroupDeltas = intFields.get(condition.field);
                if (termToGroupDeltas == null) {
                    termToGroupDeltas = new Long2ObjectOpenHashMap<Pair<IntArrayList, IntArrayList>>();
                    intFields.put(condition.field, termToGroupDeltas);
                }
                groupDeltas = termToGroupDeltas.get(condition.intTerm);
                if (groupDeltas == null) {
                    groupDeltas = Pair.of(new IntArrayList(), new IntArrayList());
                    termToGroupDeltas.put(condition.intTerm, groupDeltas);
                }
            } else {
                Map<String, Pair<IntArrayList, IntArrayList>> termToGroupDeltas = stringFields.get(condition.field);
                if (termToGroupDeltas == null) {
                    termToGroupDeltas = Maps.newHashMap();
                    stringFields.put(condition.field, termToGroupDeltas);
                }
                groupDeltas = termToGroupDeltas.get(condition.stringTerm);
                if (groupDeltas == null) {
                    groupDeltas = Pair.of(new IntArrayList(), new IntArrayList());
                    termToGroupDeltas.put(condition.stringTerm, groupDeltas);
                }
            }
            groupDeltas.getFirst().add(groups[i]);
            groupDeltas.getSecond().add(deltas[i]);
        }
        final DocIdStream docIdStream = flamdexReader.getDocIdStream();
        IntTermIterator intTermIterator = null;
        StringTermIterator stringTermIterator = null;
        try {
            for (Map.Entry<String, Long2ObjectMap<Pair<IntArrayList, IntArrayList>>> entry : intFields.entrySet()) {
                final String field = entry.getKey();
                final Long2ObjectMap<Pair<IntArrayList, IntArrayList>> termToGroupDeltas = entry.getValue();
                intTermIterator = flamdexReader.getIntTermIterator(field);
                for (Long2ObjectMap.Entry<Pair<IntArrayList, IntArrayList>> entry2 : termToGroupDeltas.long2ObjectEntrySet()) {
                    for (int i = 0; i < groupsSet.size(); i++) {
                        groupsWithCurrentTerm.clear(groupsSet.getInt(i));
                    }
                    groupsSet.clear();
                    final long term = entry2.getLongKey();
                    final Pair<IntArrayList, IntArrayList> groupDeltas = entry2.getValue();
                    final IntArrayList termGroups = groupDeltas.getFirst();
                    final IntArrayList termDeltas = groupDeltas.getSecond();
                    for (int i = 0; i < termGroups.size(); i++) {
                        final int group = termGroups.getInt(i);
                        groupsWithCurrentTerm.set(group);
                        groupToDelta[group] = termDeltas.getInt(i);
                        groupsSet.add(group);
                    }
                    intTermIterator.reset(term);
                    if (!intTermIterator.next()) continue;
                    if (intTermIterator.term() != term) continue;
                    docIdStream.reset(intTermIterator);
                    updateDocsWithTermDynamicMetric(metric, groupsWithCurrentTerm, groupToDelta, docIdStream);
                }
                intTermIterator.close();
            }
            for (Map.Entry<String, Map<String, Pair<IntArrayList, IntArrayList>>> entry : stringFields.entrySet()) {
                final String field = entry.getKey();
                final Map<String, Pair<IntArrayList, IntArrayList>> termToGroupDeltas = entry.getValue();
                stringTermIterator = flamdexReader.getStringTermIterator(field);
                for (Map.Entry<String, Pair<IntArrayList, IntArrayList>> entry2 : termToGroupDeltas.entrySet()) {
                    for (int i = 0; i < groupsSet.size(); i++) {
                        groupsWithCurrentTerm.clear(groupsSet.getInt(i));
                    }
                    groupsSet.clear();
                    final String term = entry2.getKey();
                    final Pair<IntArrayList, IntArrayList> groupDeltas = entry2.getValue();
                    final IntArrayList termGroups = groupDeltas.getFirst();
                    final IntArrayList termDeltas = groupDeltas.getSecond();
                    for (int i = 0; i < termGroups.size(); i++) {
                        final int group = termGroups.getInt(i);
                        groupsWithCurrentTerm.set(group);
                        groupToDelta[group] = termDeltas.getInt(i);
                        groupsSet.add(group);
                    }
                    stringTermIterator.reset(term);
                    if (!stringTermIterator.next()) continue;
                    if (!stringTermIterator.term().equals(term)) continue;
                    docIdStream.reset(stringTermIterator);
                    updateDocsWithTermDynamicMetric(metric, groupsWithCurrentTerm, groupToDelta, docIdStream);
                }
                stringTermIterator.close();
            }
            // pessimistically recompute all stats -- other metrics may indirectly
            // refer to this one
            for (int i = 0; i < numStats; i++) {
                needToReCalcGroupStats[i] = true;
                groupStats[i] = clearAndResize(groupStats[i], docIdToGroup.getNumGroups(), memory);
            }
        } catch (ImhotepOutOfMemoryException e) {
            throw Throwables.propagate(e);
        } finally {
            Closeables2.closeAll(log, docIdStream, intTermIterator, stringTermIterator);
        }
    }

    private void updateDocsWithTermDynamicMetric(DynamicMetric metric, FastBitSet groupsWithCurrentTerm, int[] groupToDelta, DocIdStream docIdStream) {
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            docIdToGroup.fillDocGrpBuffer(docIdBuf, docGroupBuffer, n);
            for (int i = 0; i < n; i++) {
                if (groupsWithCurrentTerm.get(docGroupBuffer[i])) {
                    metric.add(docIdBuf[i], groupToDelta[docGroupBuffer[i]]);
                }
            }
            if (n < docIdBuf.length) break;
        }
    }

    private synchronized void adjustDeltas(DynamicMetric metric, DocIdStream docIdStream, int delta) {
        while (true) {
            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
            for (int i = 0; i < n; i++) {
                metric.add(docIdBuf[i], delta);
            }
            if (n != docIdBuf.length) {
                break;
            }
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            try {
                tryClose();
            } finally {
                closed = true;
            }
        }
    }

    private void tryClose() {
        try {
            Closeables2.closeQuietly(flamdexReaderRef, log);
            while (numStats > 0) {
                popStat();
            }
            if (docIdToGroup != null) {
                final long memFreed =
                        docIdToGroup.memoryUsed() + groupDocCount.length * 4L + BUFFER_SIZE
                                * (4 + 4 + 4) + 12L * docIdToGroup.getNumGroups();
                docIdToGroup = null;
                groupDocCount = null;
                memory.releaseMemory(memFreed);
            }

            long dynamicMetricUsage = 0;
            for (DynamicMetric metric : getDynamicMetrics().values()) {
                dynamicMetricUsage += metric.memoryUsed();
            }
            getDynamicMetrics().clear();
            if (dynamicMetricUsage > 0) {
                memory.releaseMemory(dynamicMetricUsage);
            }
            if (memory.usedMemory() > 0) {
                log.error("ImhotepLocalSession is leaking! memory reserved after all memory has been freed: "
                        + memory.usedMemory());
            }

            /* clean up the optimization log */
            if (this.optimizationLog.exists()) {
                this.optimizationLog.delete();
            }
            try {
                /* close temp readers, if there are any */
                if (this.originalReader != null) {
                    this.flamdexReaderRef.close();
                    this.flamdexReader = this.originalReader;
                    this.flamdexReaderRef = this.originalReaderRef;
                }
            } catch (IOException e) {
                log.error("Could not close optimized reader");
            }

        } finally {
            Closeables2.closeQuietly(memory, log);
        }
    }

    protected void finalize() throws Throwable {
        if (!closed) {
            log.error("ImhotepLocalSession was not closed!!!!!!! stack trace at construction:",
                      constructorStackTrace);
            close();
        }
    }

    @Override
    public synchronized void resetGroups() throws ImhotepOutOfMemoryException {
        resetOptimizedReaders();
        resetGroupsTo(1);
    }

    private void resetGroupsTo(int group) throws ImhotepOutOfMemoryException {
        final long bytesToFree = docIdToGroup.memoryUsed();
        final int newNumGroups = group + 1;

        clearZeroDocBitsets();
        accountForFlamdexFTGSIteratorMemChange(docIdToGroup.getNumGroups(), newNumGroups);
        docIdToGroup = new ConstantGroupLookup(this, group, numDocs);
        recalcGroupCounts(newNumGroups);
        recalcGroupStats(newNumGroups);
        memory.releaseMemory(bytesToFree);
    }

    void clearZeroDocBitsets() {
        // Remove cache of what terms only exist in group zero
        if (fieldZeroDocBitsets != null) {
            long memoryFreed = 0L;
            for (Pair<String, Boolean> field : fieldZeroDocBitsets.keySet()) {
                memoryFreed += fieldZeroDocBitsets.get(field).memoryUsage();
            }
            fieldZeroDocBitsets.clear();
            memory.releaseMemory(memoryFreed);
        }
    }

    private static int[] clearAndResize(int[] a, int newSize, MemoryReserver memory) throws ImhotepOutOfMemoryException {
        if (a == null || newSize > a.length) {
            if (!memory.claimMemory(newSize * 4)) {
                throw new ImhotepOutOfMemoryException();
            }
            final int[] ret = new int[newSize];
            if (a != null) {
                memory.releaseMemory(a.length * 4);
            }
            return ret;
        }
        Arrays.fill(a, 0);
        return a;
    }

    private static long[] clearAndResize(long[] a, int newSize, MemoryReserver memory) throws ImhotepOutOfMemoryException {
        if (a == null || newSize > a.length) {
            if (!memory.claimMemory(newSize * 8)) {
                throw new ImhotepOutOfMemoryException();
            }
            final long[] ret = new long[newSize];
            if (a != null) {
                memory.releaseMemory(a.length * 8);
            }
            return ret;
        }
        Arrays.fill(a, 0);
        return a;
    }

    private static void updateGroupStatsAllDocs(IntValueLookup statLookup,
                                                long[] groupStats,
                                                GroupLookup docIdToGroup,
                                                int[] docGrpBuffer,
                                                int[] docIdBuf,
                                                long[] valBuf) {
        // populate new group stats
        final int numDocs = docIdToGroup.size();
        for (int start = 0; start < numDocs; start += BUFFER_SIZE) {
            final int n = Math.min(BUFFER_SIZE, numDocs - start);
            for (int i = 0; i < n; i++) {
                docIdBuf[i] = start + i;
            }
            docIdToGroup.fillDocGrpBuffer(docIdBuf, docGrpBuffer, n);
            updateGroupStatsDocIdBuf(statLookup, groupStats, docGrpBuffer, docIdBuf, valBuf, n);
        }
    }

    static void updateGroupStatsDocIdBuf(IntValueLookup statLookup,
                                         long[] groupStats,
                                         int[] docGrpBuffer,
                                         int[] docIdBuf,
                                         long[] valBuf,
                                         int n) {
        statLookup.lookup(docIdBuf, valBuf, n);
        for (int i = 0; i < n; i++) {
            groupStats[docGrpBuffer[i]] += valBuf[i];
        }
    }

    static void clear(long[] array, int[] groupsSeen, int groupsSeenCount) {
        for (int i = 0; i < groupsSeenCount; i++) {
            array[groupsSeen[i]] = 0;
        }
    }

    private static class IntFieldConditionSummary {
        long maxInequalityTerm = Long.MIN_VALUE;
        Set<Long> otherTerms = new HashSet<Long>();
    }

    private static class StringFieldConditionSummary {
        String maxInequalityTerm = null;
        Set<String> otherTerms = new HashSet<String>();
    }

    private void applyIntConditions(GroupRemapRule[] remapRules,
                                    DocIdStream docIdStream,
                                    ThreadSafeBitSet docRemapped) {
        final Map<String, IntFieldConditionSummary> intFields =
                buildIntFieldConditionSummaryMap(remapRules);
        for (final String intField : intFields.keySet()) {
            final IntFieldConditionSummary summary = intFields.get(intField);
            log.debug("Splitting groups using int field: " + intField);
            final IntTermIterator itr = flamdexReader.getIntTermIterator(intField);

            if (summary.maxInequalityTerm >= 0) {
                while (itr.next()) {
                    final long itrTerm = itr.term();
                    if (itrTerm > summary.maxInequalityTerm
                            && !summary.otherTerms.contains(itrTerm)) {
                        continue;
                    }
                    docIdStream.reset(itr);
                    do {
                        final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                        docIdToGroup.applyIntConditionsCallback(n,
                                                                docRemapped,
                                                                remapRules,
                                                                intField,
                                                                itrTerm);
                        if (n != docIdBuf.length) {
                            break;
                        }
                    } while (true);
                }
            } else {
                for (final long term : summary.otherTerms) {
                    itr.reset(term);
                    if (itr.next() && itr.term() == term) {
                        docIdStream.reset(itr);
                        do {
                            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                            docIdToGroup.applyIntConditionsCallback(n,
                                                                    docRemapped,
                                                                    remapRules,
                                                                    intField,
                                                                    term);
                            if (n != docIdBuf.length) {
                                break;
                            }
                        } while (true);
                    }
                }
            }
            itr.close();
        }
    }

    private void applyStringConditions(GroupRemapRule[] remapRules,
                                       DocIdStream docIdStream,
                                       ThreadSafeBitSet docRemapped) {
        final Map<String, StringFieldConditionSummary> stringFields =
                buildStringFieldConditionSummaryMap(remapRules);
        for (final String stringField : stringFields.keySet()) {
            final StringFieldConditionSummary summary = stringFields.get(stringField);
            log.debug("Splitting groups using string field: " + stringField);

            final StringTermIterator itr = flamdexReader.getStringTermIterator(stringField);

            if (summary.maxInequalityTerm != null) {
                while (itr.next()) {
                    final String itrTerm = itr.term();
                    if ((summary.maxInequalityTerm.compareTo(itrTerm) < 0)
                            && !summary.otherTerms.contains(itrTerm)) {
                        continue;
                    }
                    docIdStream.reset(itr);
                    do {
                        final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                        docIdToGroup.applyStringConditionsCallback(n,
                                                                   docRemapped,
                                                                   remapRules,
                                                                   stringField,
                                                                   itrTerm);
                        if (n != docIdBuf.length) {
                            break;
                        }
                    } while (true);
                }
            } else {
                for (final String term : summary.otherTerms) {
                    itr.reset(term);
                    if (itr.next() && itr.term().equals(term)) {
                        docIdStream.reset(itr);
                        do {
                            final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                            docIdToGroup.applyStringConditionsCallback(n,
                                                                       docRemapped,
                                                                       remapRules,
                                                                       stringField,
                                                                       term);
                            if (n != docIdBuf.length) {
                                break;
                            }
                        } while (true);
                    }
                }
            }
            itr.close();
        }
    }

    static boolean checkStringCondition(RegroupCondition condition,
                                        String stringField,
                                        String itrTerm) {
        if (condition == null) {
            return true;
        }
        if (condition.intType) {
            return true;
        }
        // field is interned
        if (stringField != condition.field) {
            return true;
        }
        if (condition.inequality) {
            if (itrTerm.compareTo(condition.stringTerm) > 0) {
                return true;
            }
        } else {
            if (!itrTerm.equals(condition.stringTerm)) {
                return true;
            }
        }
        return false;
    }

    static boolean checkIntCondition(RegroupCondition condition, String intField, long itrTerm) {
        if (condition == null) {
            return true;
        }
        if (!condition.intType) {
            return true;
        }
        // field is interned
        if (intField != condition.field) {
            return true;
        }
        if (condition.inequality) {
            if (itrTerm > condition.intTerm) {
                return true;
            }
        } else {
            if (itrTerm != condition.intTerm) {
                return true;
            }
        }
        return false;
    }

    private static Map<String, IntFieldConditionSummary> buildIntFieldConditionSummaryMap(GroupRemapRule[] rules) {
        final Map<String, IntFieldConditionSummary> ret =
                new HashMap<String, IntFieldConditionSummary>();
        for (final GroupRemapRule rule : rules) {
            if (rule == null) {
                continue;
            }
            final RegroupCondition condition = rule.condition;
            if (condition == null) {
                continue;
            }
            if (!condition.intType) {
                continue;
            }
            if (!condition.inequality) {
                continue;
            }

            IntFieldConditionSummary entry = ret.get(condition.field);
            if (entry == null) {
                entry = new IntFieldConditionSummary();
                ret.put(condition.field, entry);
            }
            entry.maxInequalityTerm = Math.max(entry.maxInequalityTerm, condition.intTerm);
        }
        for (final GroupRemapRule rule : rules) {
            if (rule == null) {
                continue;
            }
            final RegroupCondition condition = rule.condition;
            if (condition == null) {
                continue;
            }
            if (!condition.intType) {
                continue;
            }
            if (condition.inequality) {
                continue;
            }

            IntFieldConditionSummary entry = ret.get(condition.field);
            if (entry == null) {
                entry = new IntFieldConditionSummary();
                ret.put(condition.field, entry);
            }
            if (condition.intTerm <= entry.maxInequalityTerm) {
                continue;
            }
            entry.otherTerms.add(condition.intTerm);
        }
        return ret;
    }

    private static Map<String, StringFieldConditionSummary> buildStringFieldConditionSummaryMap(GroupRemapRule[] rules) {
        final Map<String, StringFieldConditionSummary> ret =
                new HashMap<String, StringFieldConditionSummary>();
        for (final GroupRemapRule rule : rules) {
            if (rule == null) {
                continue;
            }
            final RegroupCondition condition = rule.condition;
            if (condition == null) {
                continue;
            }
            if (condition.intType) {
                continue;
            }
            if (!condition.inequality) {
                continue;
            }

            StringFieldConditionSummary entry = ret.get(condition.field);
            if (entry == null) {
                entry = new StringFieldConditionSummary();
                ret.put(condition.field, entry);
            }
            entry.maxInequalityTerm = stringMax(entry.maxInequalityTerm, condition.stringTerm);
        }
        for (final GroupRemapRule rule : rules) {
            if (rule == null) {
                continue;
            }
            final RegroupCondition condition = rule.condition;
            if (condition == null) {
                continue;
            }
            if (condition.intType) {
                continue;
            }
            if (condition.inequality) {
                continue;
            }

            StringFieldConditionSummary entry = ret.get(condition.field);
            if (entry == null) {
                entry = new StringFieldConditionSummary();
                ret.put(condition.field, entry);
            }
            if (entry.maxInequalityTerm != null
                    && condition.stringTerm.compareTo(entry.maxInequalityTerm) <= 0) {
                continue;
            }
            entry.otherTerms.add(condition.stringTerm);
        }
        return ret;
    }

    private static String stringMax(String a, String b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        if (a.compareTo(b) >= 0) {
            return a;
        }
        return b;
    }

    private static enum Metric {
        COUNT("count()"),
        CACHED("cached()"),
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULUS("%"),
        ABSOLUTE_VALUE("abs()"),
        MIN("min()"),
        MAX("max()"),
        EQ("="),
        NE("!="),
        LT("<"),
        LTE("<="),
        GT(">"),
        GTE(">=");

        private final String key;

        private Metric(final String key) {
            this.key = key;
        }

        private static final Map<String, Metric> map;
        static {
            final ImmutableMap.Builder<String, Metric> builder = ImmutableMap.builder();
            for (final Metric metric : Metric.values()) {
                builder.put(metric.key, metric);
            }
            map = builder.build();
        }

        static Metric getMetric(final String statName) {
            return map.get(statName);
        }
    }

    private IntValueLookup hasIntTermFilter(final String field, final long term) throws ImhotepOutOfMemoryException {
        final long memoryUsage = getBitSetMemoryUsage();

        if (!memory.claimMemory(memoryUsage)) {
            throw new ImhotepOutOfMemoryException();
        }

        return new BitSetIntValueLookup(FlamdexUtils.cacheHasIntTerm(field, term, flamdexReader),
                                        memoryUsage);
    }

    private IntValueLookup hasStringTermFilter(final String field, final String term) throws ImhotepOutOfMemoryException {
        final long memoryUsage = getBitSetMemoryUsage();

        if (!memory.claimMemory(memoryUsage)) {
            throw new ImhotepOutOfMemoryException();
        }

        return new BitSetIntValueLookup(
                                        FlamdexUtils.cacheHasStringTerm(field, term, flamdexReader),
                                        memoryUsage);
    }

    private IntValueLookup intTermCountLookup(final String field) throws ImhotepOutOfMemoryException {
        final long memoryUsage = flamdexReader.getNumDocs();

        if (!memory.claimMemory(memoryUsage)) {
            throw new ImhotepOutOfMemoryException();
        }

        final byte[] array = new byte[flamdexReader.getNumDocs()];

        final IntTermIterator iterator = flamdexReader.getIntTermIterator(field);
        try {
            final DocIdStream docIdStream = flamdexReader.getDocIdStream();
            try {
                while (iterator.next()) {
                    docIdStream.reset(iterator);
                    while (true) {
                        final int n = docIdStream.fillDocIdBuffer(docIdBuf);
                        for (int i = 0; i < n; ++i) {
                            final int doc = docIdBuf[i];
                            if (array[doc] != (byte) 255) {
                                ++array[doc];
                            }
                        }
                        if (n < BUFFER_SIZE) {
                            break;
                        }
                    }
                }
            } finally {
                docIdStream.close();
            }
        } finally {
            iterator.close();
        }

        return new MemoryReservingIntValueLookupWrapper(new ByteArrayIntValueLookup(array, 0, 255));
    }

    private IntValueLookup stringTermCountLookup(final String field) throws ImhotepOutOfMemoryException {
        final long memoryUsage = flamdexReader.getNumDocs();

        if (!memory.claimMemory(memoryUsage)) {
            throw new ImhotepOutOfMemoryException();
        }

        final byte[] array = new byte[flamdexReader.getNumDocs()];

        final StringTermDocIterator iterator = flamdexReader.getStringTermDocIterator(field);
        try {
            while (iterator.nextTerm()) {
                while (true) {
                    final int n = iterator.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; ++i) {
                        final int doc = docIdBuf[i];
                        if (array[doc] != (byte) 255) {
                            ++array[doc];
                        }
                    }
                    if (n < BUFFER_SIZE) {
                        break;
                    }
                }
            }
        } finally {
            Closeables2.closeQuietly(iterator, log);
        }

        return new MemoryReservingIntValueLookupWrapper(new ByteArrayIntValueLookup(array, 0, 255));
    }

    private IntValueLookup scaledFloatLookup(final String field, double scale, double offset) throws ImhotepOutOfMemoryException {
        final long memoryUsage = 4 * flamdexReader.getNumDocs();

        if (!memory.claimMemory(memoryUsage)) {
            throw new ImhotepOutOfMemoryException();
        }

        final int[] array = new int[flamdexReader.getNumDocs()];
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        final StringTermDocIterator iterator = flamdexReader.getStringTermDocIterator(field);
        try {
            while (iterator.nextTerm()) {
                final String term = iterator.term();
                int number;
                try {
                    final double termFloat = Double.parseDouble(term);
                    number = (int) Math.round(termFloat * scale + offset);
                } catch (NumberFormatException e) {
                    number = 0;
                }

                min = Math.min(min, number);
                max = Math.max(max, number);

                while (true) {
                    final int n = iterator.fillDocIdBuffer(docIdBuf);
                    for (int i = 0; i < n; i++) {
                        final int doc = docIdBuf[i];
                        array[doc] = number;
                    }
                    if (n < BUFFER_SIZE) {
                        break;
                    }
                }
            }
        } finally {
            Closeables2.closeQuietly(iterator, log);
        }

        return new MemoryReservingIntValueLookupWrapper(new IntArrayIntValueLookup(array, min, max));
    }

    private int getBitSetMemoryUsage() {
        return flamdexReader.getNumDocs() / 8 + ((flamdexReader.getNumDocs() % 8) != 0 ? 1 : 0);
    }

    private class BitSetIntValueLookup implements IntValueLookup {
        private ThreadSafeBitSet bitSet;
        private final long memoryUsage;

        private BitSetIntValueLookup(ThreadSafeBitSet bitSet, long memoryUsage) {
            this.bitSet = bitSet;
            this.memoryUsage = memoryUsage;
        }

        @Override
        public long getMin() {
            return 0;
        }

        @Override
        public long getMax() {
            return 1;
        }

        @Override
        public void lookup(int[] docIds, long[] values, int n) {
            for (int i = 0; i < n; ++i) {
                values[i] = bitSet.get(docIds[i]) ? 1 : 0;
            }
        }

        @Override
        public long memoryUsed() {
            return 0;
        }

        @Override
        public void close() {
            bitSet = null;
            memory.releaseMemory(memoryUsage);
        }
    }

    private final class MemoryReservingIntValueLookupWrapper implements IntValueLookup {
        final IntValueLookup lookup;

        private MemoryReservingIntValueLookupWrapper(final IntValueLookup lookup) {
            this.lookup = lookup;
        }

        @Override
        public long getMin() {
            return lookup.getMin();
        }

        @Override
        public long getMax() {
            return lookup.getMax();
        }

        @Override
        public void lookup(final int[] docIds, final long[] values, final int n) {
            lookup.lookup(docIds, values, n);
        }

        @Override
        public long memoryUsed() {
            return lookup.memoryUsed();
        }

        @Override
        public void close() {
            final long usedMemory = memoryUsed();
            lookup.close();
            memory.releaseMemory(usedMemory);
        }
    }

}
