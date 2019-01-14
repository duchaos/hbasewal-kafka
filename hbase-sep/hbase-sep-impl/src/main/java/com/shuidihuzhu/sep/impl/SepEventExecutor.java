/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shuidihuzhu.sep.impl;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.shuidihuzhu.sep.EventListener;
import com.shuidihuzhu.sep.SepEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Executes SepEvents in batches over multiple threads. All events for the same row will be executed by the same thread,
 * and will be batched in the order that they were received in.
 * <p>
 * As SepEvents are executed in batches, after scheduling they will be buffered until their batch size is reached or the
 * {@code flush} method is called.
 * <p>
 * Note that although this class uses multiple threads, its use is not thread-safe. Events should only be scheduled from
 * within a single thread.
 */
public class SepEventExecutor {

    private Log log = LogFactory.getLog(getClass());
    private EventListener eventListener;
    private int numThreads;
    private int batchSize;
    private SepMetrics sepMetrics;
    private List<ThreadPoolExecutor> executors;
    private Multimap<Integer, SepEvent> eventBuffers;
    private List<Future<?>> futures;
    private HashFunction hashFunction = Hashing.murmur3_32();
    private boolean stopped = false;

    public SepEventExecutor(EventListener eventListener, List<ThreadPoolExecutor> executors, int batchSize, SepMetrics sepMetrics) {
        this.eventListener = eventListener;
        this.executors = executors;
        this.numThreads = executors.size();
        this.batchSize = batchSize;
        this.sepMetrics = sepMetrics;
        eventBuffers = ArrayListMultimap.create(numThreads, batchSize);
        futures = Lists.newArrayList();
    }

    /**
     * Schedule a {@link SepEvent} for execution.
     * <p>
     * The event will be buffered until it can be executed within a batch of the configured batch size, or until the
     * {@link #flush()} method is called.
     * 
     * @param sepEvent event to be scheduled
     */
    public void scheduleSepEvent(SepEvent sepEvent) {

        if (stopped) {
            throw new IllegalStateException("This executor is stopped");
        }

        // We don't want messages of the same row to be processed concurrently, therefore choose
        // a thread based on the hash of the row key
        int partition = (hashFunction.hashBytes(sepEvent.getRow()).asInt() & Integer.MAX_VALUE) % numThreads;
        List<SepEvent> eventBuffer = (List<SepEvent>)eventBuffers.get(partition);
        eventBuffer.add(sepEvent);
        if (eventBuffer.size() == batchSize) {
            scheduleEventBatch(partition, Lists.newArrayList(eventBuffer));
            eventBuffers.removeAll(partition);
        }
    }

    private void scheduleEventBatch(int partition, final List<SepEvent> events) {
        Future<?> future = executors.get(partition).submit(new Runnable() {
            @Override
            public void run() {
                try {
                    long before = System.currentTimeMillis();
                    log.debug("Delivering message to listener");
                    eventListener.processEvents(events);
                    sepMetrics.reportFilteredSepOperation(System.currentTimeMillis() - before);
                } catch (RuntimeException e) {
                    log.error("Error while processing event", e);
                    throw e;
                }
            }
        });
        futures.add(future);
    }

    /**
     * Flush all buffered SepEvent batches, causing them to be started up for execution.
     * <p>
     * Returns all {@code Future}s for all events that have been scheduled since the last time this method was called.
     */
    public List<Future<?>> flush() {
        for (int partition : eventBuffers.keySet()) {
            List<SepEvent> buffer = (List<SepEvent>)eventBuffers.get(partition);
            if (!buffer.isEmpty()) {
                scheduleEventBatch(partition, Lists.newArrayList(buffer));
            }
        }
        eventBuffers.clear();
        List<Future<?>> flushedFutures = Lists.newArrayList(futures);
        return flushedFutures;
    }

}
