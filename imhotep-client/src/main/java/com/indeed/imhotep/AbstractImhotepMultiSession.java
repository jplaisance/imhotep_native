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
 package com.indeed.imhotep;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.util.core.Throwables2;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.threads.LogOnUncaughtExceptionHandler;
import com.indeed.flamdex.query.Term;
import com.indeed.flamdex.utils.BlockingCopyableIterator;
import com.indeed.imhotep.api.DocIterator;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.api.RawFTGSIterator;
import com.indeed.imhotep.service.DocIteratorMerger;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import org.apache.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author jsgroth
 */
public abstract class AbstractImhotepMultiSession extends AbstractImhotepSession {
    private static final Logger log = Logger.getLogger(AbstractImhotepMultiSession.class);

    protected final ImhotepSession[] sessions;

    private final Long[] totalDocFreqBuf;

    private final Integer[] integerBuf;

    private final Long[] longBuf;

    private final Object[] nullBuf;

    private final long[][] groupStatsBuf;

    private final List<TermCount>[] termCountListBuf;

    private FTGSIterator lastIterator;

    private final ExecutorService getSplitBufferThreads = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("FTGS-Buffer-Thread-getSplit-%d")
                    .setUncaughtExceptionHandler(new LogOnUncaughtExceptionHandler(log))
                    .build()
    );

    private final ExecutorService mergeSplitBufferThreads = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("FTGS-Buffer-Thread-mergeSplit-%d")
                    .setUncaughtExceptionHandler(new LogOnUncaughtExceptionHandler(log))
                    .build()
    );

    protected int numStats = 0;

    private int numGroups = 2;

    @SuppressWarnings({"unchecked"})
    protected AbstractImhotepMultiSession(ImhotepSession[] sessions) {
        if (sessions == null || sessions.length == 0) {
            throw new IllegalArgumentException("at least one session is required");
        }

        this.sessions = sessions;

        totalDocFreqBuf = new Long[sessions.length];
        integerBuf = new Integer[sessions.length];
        longBuf = new Long[sessions.length];
        nullBuf = new Object[sessions.length];
        groupStatsBuf = new long[sessions.length][];
        termCountListBuf = new List[sessions.length];
    }

    @Override
    public long getTotalDocFreq(final String[] intFields, final String[] stringFields) {
        executeRuntimeException(totalDocFreqBuf, new ThrowingFunction<ImhotepSession, Long>() {
            @Override
            public Long apply(ImhotepSession session) throws Exception {
                return session.getTotalDocFreq(intFields, stringFields);
            }
        });
        long sum = 0L;
        for (final long totalDocFreq : totalDocFreqBuf) {
            sum += totalDocFreq;
        }
        return sum;
    }

    @Override
    public long[] getGroupStats(final int stat) {
        executeRuntimeException(groupStatsBuf, new ThrowingFunction<ImhotepSession, long[]>() {
            @Override
            public long[] apply(ImhotepSession session) throws Exception {
                return session.getGroupStats(stat);
            }
        });

        int numGroups = 0;
        for (int i = 0; i < sessions.length; ++i) {
            final long[] statsBuf = groupStatsBuf[i];
            final int statsBufLength = statsBuf.length;
            numGroups = Math.max(numGroups, statsBufLength);
        }

        final long[] totalStats = new long[numGroups];
        for (final long[] stats : groupStatsBuf) {
            for (int group = 1; group < stats.length; ++group) {
                totalStats[group] += stats[group];
            }
        }
        return totalStats;
    }

    @Override
    public int regroup(final GroupMultiRemapRule[] rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup(rawRules, errorOnCollisions);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public int regroup(final int numRawRules, final Iterator<GroupMultiRemapRule> rawRules, final boolean errorOnCollisions) throws ImhotepOutOfMemoryException {
        final BlockingCopyableIterator<GroupMultiRemapRule> copyableIterator = new BlockingCopyableIterator<GroupMultiRemapRule>(rawRules, sessions.length, 256);

        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup(numRawRules, copyableIterator.iterator(), errorOnCollisions);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public int regroup(final GroupRemapRule[] rawRules) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup(rawRules);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    public int regroup2(final int numRules, final Iterator<GroupRemapRule> rules) throws ImhotepOutOfMemoryException {
        final BlockingCopyableIterator<GroupRemapRule> copyableIterator = new BlockingCopyableIterator<GroupRemapRule>(rules, sessions.length, 256);
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup2(numRules, copyableIterator.iterator());
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public int regroup(final QueryRemapRule rule) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.regroup(rule);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public void intOrRegroup(final String field, final long[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession session) throws Exception {
                session.intOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void stringOrRegroup(final String field, final String[] terms, final int targetGroup, final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession session) throws Exception {
                session.stringOrRegroup(field, terms, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void randomRegroup(final String field, final boolean isIntField, final String salt, final double p, final int targetGroup,
                              final int negativeGroup, final int positiveGroup) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession session) throws Exception {
                session.randomRegroup(field, isIntField, salt, p, targetGroup, negativeGroup, positiveGroup);
                return null;
            }
        });
    }

    @Override
    public void randomMultiRegroup(final String field, final boolean isIntField, final String salt, final int targetGroup,
                                   final double[] percentages, final int[] resultGroups) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession session) throws Exception {
                session.randomMultiRegroup(field, isIntField, salt, targetGroup, percentages, resultGroups);
                return null;
            }
        });
    }

    @Override
    public int metricRegroup(final int stat, final long min, final long max, final long intervalSize, final boolean noGutters) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.metricRegroup(stat, min, max, intervalSize, noGutters);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public int metricRegroup2D(final int xStat, final long xMin, final long xMax, final long xIntervalSize,
                               final int yStat, final long yMin, final long yMax, final long yIntervalSize) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.metricRegroup2D(xStat, xMin, xMax, xIntervalSize, yStat, yMin, yMax, yIntervalSize);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    public int metricFilter(final int stat, final long min, final long max, final boolean negate) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.metricFilter(stat, min, max, negate);
            }
        });

        numGroups = Collections.max(Arrays.asList(integerBuf));
        return numGroups;
    }

    @Override
    public List<TermCount> approximateTopTerms(final String field, final boolean isIntField, final int k) {
        final int subSessionK = k * 2;

        executeRuntimeException(termCountListBuf, new ThrowingFunction<ImhotepSession, List<TermCount>>() {
            @Override
            public List<TermCount> apply(ImhotepSession session) throws Exception {
                return session.approximateTopTerms(field, isIntField, subSessionK);
            }
        });

        return mergeTermCountLists(termCountListBuf, field, isIntField, k);
    }

    private static List<TermCount> mergeTermCountLists(List<TermCount>[] termCountListBuf, String field, boolean isIntField, int k) {
        final List<TermCount> ret;
        if (isIntField) {
            final Long2LongMap counts = new Long2LongOpenHashMap(k * 2);
            for (final List<TermCount> list : termCountListBuf) {
                for (final TermCount termCount : list) {
                    final long termVal = termCount.getTerm().getTermIntVal();
                    final long count = termCount.getCount();
                    counts.put(termVal, counts.get(termVal) + count);
                }
            }
            ret = Lists.newArrayListWithCapacity(counts.size());
            for (final LongIterator iter = counts.keySet().iterator(); iter.hasNext(); ) {
                final Long term = iter.nextLong();
                final long count = counts.get(term);
                ret.add(new TermCount(new Term(field, isIntField, term, ""), count));
            }
        } else {
            final Object2LongMap<String> counts = new Object2LongOpenHashMap<String>(k * 2);
            for (final List<TermCount> list : termCountListBuf) {
                for (final TermCount termCount : list) {
                    final String termVal = termCount.getTerm().getTermStringVal();
                    final long count = termCount.getCount();
                    counts.put(termVal, counts.getLong(termVal) + count);
                }
            }
            ret = Lists.newArrayListWithCapacity(counts.size());
            for (final String term : counts.keySet()) {
                final long count = counts.get(term);
                ret.add(new TermCount(new Term(field, isIntField, 0, term), count));
            }
        }
        Collections.sort(ret, TermCount.REVERSE_COUNT_COMPARATOR);
        final int end = Math.min(k, ret.size());
        return ret.subList(0, end);
    }

    @Override
    public int pushStat(final String statName) throws ImhotepOutOfMemoryException {
        executeMemoryException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.pushStat(statName);
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int pushStats(final List<String> statNames) throws ImhotepOutOfMemoryException {
        executeRuntimeException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.pushStats(statNames);
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int popStat() {
        executeRuntimeException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.popStat();
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public int getNumStats() {
        executeRuntimeException(integerBuf, new ThrowingFunction<ImhotepSession, Integer>() {
            @Override
            public Integer apply(ImhotepSession session) throws Exception {
                return session.getNumStats();
            }
        });

        numStats = validateNumStats(integerBuf);
        return numStats;
    }

    @Override
    public long getLowerBound(final int stat) {
        executeRuntimeException(longBuf, new ThrowingFunction<ImhotepSession, Long>() {
            @Override
            public Long apply(ImhotepSession session) throws Exception {
                return session.getLowerBound(stat);
            }
        });
        return Collections.min(Arrays.asList(longBuf));
    }

    @Override
    public long getUpperBound(final int stat) {
        executeRuntimeException(longBuf, new ThrowingFunction<ImhotepSession, Long>() {
            @Override
            public Long apply(ImhotepSession session) throws Exception {
                return session.getUpperBound(stat);
            }
        });
        return Collections.max(Arrays.asList(longBuf));
    }

    @Override
    public void createDynamicMetric(final String name) throws ImhotepOutOfMemoryException {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.createDynamicMetric(name);
                return null;
            }
        });
    }

    @Override
    public void updateDynamicMetric(final String name, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.updateDynamicMetric(name, deltas);
                return null;
            }
        });
    }

    @Override
    public void conditionalUpdateDynamicMetric(final String name, final RegroupCondition[] conditions, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.conditionalUpdateDynamicMetric(name, conditions, deltas);
                return null;
            }
        });
    }

    @Override
    public void groupConditionalUpdateDynamicMetric(final String name, final int[] groups, final RegroupCondition[] conditions, final int[] deltas) {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.groupConditionalUpdateDynamicMetric(name, groups, conditions, deltas);
                return null;
            }
        });
    }

    private static int validateNumStats(final Integer[] numStatBuf) {
        final int newNumStats = numStatBuf[0];
        for (int i = 1; i < numStatBuf.length; ++i) {
            if (numStatBuf[i] != newNumStats) {
                throw new RuntimeException("bug, one session did not return the same number of stats as the others");
            }
        }
        return newNumStats;
    }

    @Override
    public FTGSIterator getFTGSIterator(final String[] intFields, final String[] stringFields) {
        if (sessions.length == 1) return sessions[0].getFTGSIterator(intFields, stringFields);
        final RawFTGSIterator[] iterators = new RawFTGSIterator[sessions.length];
        executeRuntimeException(iterators, new ThrowingFunction<ImhotepSession, RawFTGSIterator>() {
            public RawFTGSIterator apply(final ImhotepSession imhotepSession) throws Exception {
                return persist(imhotepSession.getFTGSIterator(intFields, stringFields));
            }
        });
        return new RawFTGSMerger(Arrays.asList(iterators), numStats, null);
    }

    @Override
    public FTGSIterator getSubsetFTGSIterator(final Map<String, long[]> intFields, final Map<String, String[]> stringFields) {
        if (sessions.length == 1) return sessions[0].getSubsetFTGSIterator(intFields, stringFields);
        final RawFTGSIterator[] iterators = new RawFTGSIterator[sessions.length];
        executeRuntimeException(iterators, new ThrowingFunction<ImhotepSession, RawFTGSIterator>() {
            public RawFTGSIterator apply(final ImhotepSession imhotepSession) throws Exception {
                return persist(imhotepSession.getSubsetFTGSIterator(intFields, stringFields));
            }
        });
        return new RawFTGSMerger(Arrays.asList(iterators), numStats, null);
    }

    public final DocIterator getDocIterator(String[] intFields, String[] stringFields) throws ImhotepOutOfMemoryException {
        final Closer closer = Closer.create();
        try {
            final List<DocIterator> docIterators = Lists.newArrayList();
            for (ImhotepSession session : sessions) {
                docIterators.add(closer.register(session.getDocIterator(intFields, stringFields)));
            }
            return new DocIteratorMerger(docIterators, intFields.length, stringFields.length);
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables2.propagate(t, ImhotepOutOfMemoryException.class);
        }
    }

    public RawFTGSIterator getFTGSIteratorSplit(final String[] intFields, final String[] stringFields, final int splitIndex, final int numSplits) {
        final RawFTGSIterator[] splits = new RawFTGSIterator[sessions.length];
        executeRuntimeException(splits, new ThrowingFunction<ImhotepSession, RawFTGSIterator>() {
            public RawFTGSIterator apply(final ImhotepSession imhotepSession) throws Exception {
                return imhotepSession.getFTGSIteratorSplit(intFields, stringFields, splitIndex, numSplits);
            }
        });
        return new RawFTGSMerger(Arrays.asList(splits), numStats, null);
    }

    @Override
    public RawFTGSIterator getSubsetFTGSIteratorSplit(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final int splitIndex, final int numSplits) {
        final RawFTGSIterator[] splits = new RawFTGSIterator[sessions.length];
        executeRuntimeException(splits, new ThrowingFunction<ImhotepSession, RawFTGSIterator>() {
            public RawFTGSIterator apply(final ImhotepSession imhotepSession) throws Exception {
                return imhotepSession.getSubsetFTGSIteratorSplit(intFields, stringFields, splitIndex, numSplits);
            }
        });
        return new RawFTGSMerger(Arrays.asList(splits), numStats, null);
    }

    public RawFTGSIterator mergeFTGSSplit(final String[] intFields, final String[] stringFields, final String sessionId, final InetSocketAddress[] nodes, final int splitIndex) {
        final RawFTGSIterator[] splits = new RawFTGSIterator[nodes.length];
        try {
            execute(splits, nodes, new ThrowingFunction<InetSocketAddress, RawFTGSIterator>() {
                public RawFTGSIterator apply(final InetSocketAddress node) throws Exception {
                    final ImhotepRemoteSession remoteSession = new ImhotepRemoteSession(node.getHostName(), node.getPort(), sessionId);
                    remoteSession.setNumStats(numStats);
                    return remoteSession.getFTGSIteratorSplit(intFields, stringFields, splitIndex, nodes.length);
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
        return mergeFTGSSplits(splits);
    }

    @Override
    public RawFTGSIterator mergeSubsetFTGSSplit(final Map<String, long[]> intFields, final Map<String, String[]> stringFields, final String sessionId, final InetSocketAddress[] nodes, final int splitIndex) {
        final RawFTGSIterator[] splits = new RawFTGSIterator[nodes.length];
        try {
            execute(splits, nodes, new ThrowingFunction<InetSocketAddress, RawFTGSIterator>() {
                public RawFTGSIterator apply(final InetSocketAddress node) throws Exception {
                    final ImhotepRemoteSession remoteSession = new ImhotepRemoteSession(node.getHostName(), node.getPort(), sessionId);
                    remoteSession.setNumStats(numStats);
                    return remoteSession.getSubsetFTGSIteratorSplit(intFields, stringFields, splitIndex, nodes.length);
                }
            });
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
        return mergeFTGSSplits(splits);
    }

    private RawFTGSIterator mergeFTGSSplits(RawFTGSIterator[] splits) {
        final Closer closer = Closer.create();
        try {
            final RawFTGSIterator[][] iteratorSplits = new RawFTGSIterator[splits.length][];
            final int numSplits = Math.max(1, Runtime.getRuntime().availableProcessors()/2);
            for (int i = 0; i < splits.length; i++) {
                final FTGSSplitter splitter = closer.register(new FTGSSplitter(splits[i], numSplits, numStats, "mergeFtgsSplit", 981044833));
                iteratorSplits[i] = splitter.getFtgsIterators();
            }
            final RawFTGSIterator[] mergers = new RawFTGSIterator[numSplits];
            for (int j = 0; j < numSplits; j++) {
                final List<RawFTGSIterator> iterators = Lists.newArrayList();
                for (int i = 0; i < splits.length; i++) {
                    iterators.add(iteratorSplits[i][j]);
                }
                mergers[j] = closer.register(new RawFTGSMerger(iterators, numStats, null));
            }
            final RawFTGSIterator[] iterators = new RawFTGSIterator[numSplits];
            execute(iterators, mergers, new ThrowingFunction<RawFTGSIterator, RawFTGSIterator>() {
                public RawFTGSIterator apply(final RawFTGSIterator iterator) throws Exception {
                    return persist(iterator);
                }
            });
            return new FTGSInterleaver(iterators);
//            return new RawFTGSMerger(Arrays.asList(splits), numStats, null);
        } catch (Throwable t) {
            Closeables2.closeQuietly(closer, log);
            throw Throwables.propagate(t);
        }
    }

    private RawFTGSIterator persist(final FTGSIterator iterator) throws IOException {
        final File tmp = File.createTempFile("ftgs", ".tmp");
        OutputStream out = null;
        try {
            final long start = System.currentTimeMillis();
            out = new BufferedOutputStream(new FileOutputStream(tmp));
            FTGSOutputStreamWriter.write(iterator, numStats, out);
            log.info("time to merge splits to file: "+(System.currentTimeMillis()-start)+" ms, file length: "+tmp.length());
        } catch (Throwable t) {
            tmp.delete();
            throw Throwables2.propagate(t, IOException.class);
        } finally {
            Closeables2.closeQuietly(iterator, log);
            if (out != null) {
                out.close();
            }
        }
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(tmp));
        tmp.delete();
        final InputStream in = new FilterInputStream(bufferedInputStream) {
            public void close() throws IOException {
                bufferedInputStream.close();
            }
        };
        return new InputStreamFTGSIterator(in, numStats);
    }

    public RawFTGSIterator[] getFTGSIteratorSplits(final String[] intFields, final String[] stringFields) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rebuildAndFilterIndexes(final List<String> intFields, 
                                final List<String> stringFields) throws ImhotepOutOfMemoryException {
        executeMemoryException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.rebuildAndFilterIndexes(intFields, stringFields);
                return null;
          }
        });
    }
    
    @Override
    public final void close() {
        try {
            preClose();
        } finally {
            try {
                executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
                    @Override
                    public Object apply(ImhotepSession imhotepSession) throws Exception {
                        imhotepSession.close();
                        return null;
                    }
                });
            } finally {
                postClose();
            }
        }
    }

    @Override
    public void resetGroups() {
        executeRuntimeException(nullBuf, new ThrowingFunction<ImhotepSession, Object>() {
            @Override
            public Object apply(ImhotepSession imhotepSession) throws Exception {
                imhotepSession.resetGroups();
                return null;
            }
        });
    }

    protected void preClose() {
        try {
            if (lastIterator != null) {
                Closeables2.closeQuietly(lastIterator, log);
                lastIterator = null;
            }
        } finally {
            getSplitBufferThreads.shutdown();
            mergeSplitBufferThreads.shutdown();
        }
    }

    protected abstract void postClose();

    protected <T> void executeRuntimeException(final T[] ret, final ThrowingFunction<? super ImhotepSession, ? extends T> function) {
        try {
            executeSessions(ret, function);
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }

    protected <T> void executeMemoryException(final T[] ret, final ThrowingFunction<? super ImhotepSession, ? extends T> function) throws ImhotepOutOfMemoryException {
        try {
            executeSessions(ret, function);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof ImhotepOutOfMemoryException) {
                throw new ImhotepOutOfMemoryException(cause);
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

    protected abstract <E,T> void execute(final T[] ret, E[] things, final ThrowingFunction<? super E, ? extends T> function) throws ExecutionException;

    protected <T> void executeSessions(final T[] ret, final ThrowingFunction<? super ImhotepSession, ? extends T> function) throws ExecutionException {
        execute(ret, sessions, function);
    }

    protected static interface ThrowingFunction<K, V> {
        V apply(K k) throws Exception;
    }

    protected void safeClose() {
        try {
            close();
        } catch (Exception e) {
            log.error("error closing session", e);
        }
    }

    protected static final class FTGSInterleaver implements RawFTGSIterator {

        private final RawFTGSIterator[] iterators;

        private int numFieldIterators;

        private String fieldName;
        private boolean fieldIsIntType;

        private boolean done = false;

        private boolean initialized = false;

        protected FTGSInterleaver(final RawFTGSIterator[] iterators) {
            this.iterators = iterators;
        }

        @Override
        public final boolean nextField() {
            if (done) return false;

            numFieldIterators = 0;

            final FTGSIterator first = iterators[0];
            if (!first.nextField()) {
                for (int i = 1; i < iterators.length; ++i) {
                    if (iterators[i].nextField()) {
                        throw new IllegalArgumentException("sub iterator fields do not match");
                    }
                }
                close();
                return false;
            }
            fieldName = first.fieldName();
            fieldIsIntType = first.fieldIsIntType();
            numFieldIterators = iterators.length;

            for (int i = 1; i < iterators.length; ++i) {
                final FTGSIterator itr = iterators[i];
                if (!itr.nextField() || !itr.fieldName().equals(fieldName) || itr.fieldIsIntType() != fieldIsIntType) {
                    throw new IllegalArgumentException("sub iterator fields do not match");
                }
            }
            initialized = false;
            return true;
        }

        @Override
        public final String fieldName() {
            return fieldName;
        }

        @Override
        public final boolean fieldIsIntType() {
            return fieldIsIntType;
        }

        @Override
        public boolean nextTerm() {
            if (!initialized) {
                initialized = true;
                for (int i = iterators.length-1; i >= 0; i--) {
                    if (!iterators[i].nextTerm()) {
                        numFieldIterators--;
                        swap(i, numFieldIterators);
                    }
                }
                for (int i = numFieldIterators-1; i >= 0; i--) {
                    downHeap(i);
                }
                return numFieldIterators > 0;
            }
            if (numFieldIterators <= 0) return false;
            if (!iterators[0].nextTerm()) {
                numFieldIterators--;
                if (numFieldIterators <= 0) return false;
                swap(0, numFieldIterators);
            }
            downHeap(0);
            return true;
        }

        @Override
        public long termDocFreq() {
            return iterators[0].termDocFreq();
        }

        @Override
        public long termIntVal() {
            return iterators[0].termIntVal();
        }

        @Override
        public String termStringVal() {
            return iterators[0].termStringVal();
        }

        @Override
        public byte[] termStringBytes() {
            return iterators[0].termStringBytes();
        }

        @Override
        public int termStringLength() {
            return iterators[0].termStringLength();
        }

        @Override
        public boolean nextGroup() {
            return iterators[0].nextGroup();
        }

        @Override
        public int group() {
            return iterators[0].group();
        }

        @Override
        public void groupStats(final long[] stats) {
            iterators[0].groupStats(stats);
        }

        @Override
        public void close() {
            if (!done) {
                done = true;
                for (RawFTGSIterator iterator : iterators) {
                    iterator.close();
                }
            }
        }

        private void downHeap(int index) {
            while (true) {
                final int leftIndex = index * 2 + 1;
                final int rightIndex = leftIndex+1;
                if (leftIndex < numFieldIterators) {
                    if (fieldIsIntType) {
                        final int lowerIndex = rightIndex >= numFieldIterators ?
                                leftIndex :
                                (compareIntTerms(leftIndex, rightIndex) <= 0 ?
                                        leftIndex :
                                        rightIndex);
                        if (compareIntTerms(lowerIndex, index) < 0) {
                            swap(index, lowerIndex);
                            index = lowerIndex;
                        } else {
                            break;
                        }
                    } else {
                        final int lowerIndex;
                        if (rightIndex >= numFieldIterators) {
                            lowerIndex = leftIndex;
                        } else {
                            lowerIndex = compareStringTerms(leftIndex, rightIndex) <= 0 ?
                                    leftIndex :
                                    rightIndex;
                        }
                        if (compareStringTerms(lowerIndex, index) < 0) {
                            swap(index, lowerIndex);
                            index = lowerIndex;
                        } else {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }

        private int compareIntTerms(int a, int b) {
            return Longs.compare(iterators[a].termIntVal(), iterators[b].termIntVal());
        }

        private int compareStringTerms(int a, int b) {
            final RawFTGSIterator itA = iterators[a];
            final RawFTGSIterator itB = iterators[b];
            return RawFTGSMerger.compareBytes(itA.termStringBytes(), itA.termStringLength(), itB.termStringBytes(), itB.termStringLength());
        }

        private void swap(int a, int b) {
            final RawFTGSIterator tmp = iterators[a];
            iterators[a] = iterators[b];
            iterators[b] = tmp;
        }
    }
}
