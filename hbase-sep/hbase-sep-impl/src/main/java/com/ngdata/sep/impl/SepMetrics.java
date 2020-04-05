/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep.impl;

import javax.management.ObjectName;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsDynamicMBeanBase;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

/**
 * Metrics for the Side-Effect Processor (SEP) system.
 */
public class SepMetrics implements Updater {

    private final String recordName;
    private final MetricsRegistry metricsRegistry;
    private final MetricsRecord metricsRecord;
    private final MetricsContext context;
    private final SepMetricsMXBean mbean;

    // Processing rate for SEP actions for which we actually do something (i.e. after filtering)
    private final MetricsTimeVaryingRate sepProcessingRate;

    // The write timestamp of the last SEP information that came in
    private final MetricsLongValue lastTimestampInputProcessed;

    public SepMetrics(String recordName) {
        this.recordName = recordName;
        metricsRegistry = new MetricsRegistry();
        sepProcessingRate = new MetricsTimeVaryingRate("sepProcessed", metricsRegistry);
        lastTimestampInputProcessed = new MetricsLongValue("lastSepTimestamp", metricsRegistry);

        context = MetricsUtil.getContext("repository");
        metricsRecord = MetricsUtil.createRecord(context, recordName);
        context.registerUpdater(this);
        mbean = new SepMetricsMXBean(this.metricsRegistry);
    }

    public void shutdown() {
        context.unregisterUpdater(this);
        mbean.shutdown();
    }

    /**
     * Report that a filtered SEP operation has been processed. This method should only be called to
     * report SEP operations that have been processed after making it through the filtering process.
     * 
     * @param duration The number of millisecods spent handling the SEP operation
     */
    public void reportFilteredSepOperation(long duration) {
        sepProcessingRate.inc(duration);
    }

    /**
     * Report the original write timestamp of a SEP operation that was received. Assuming that SEP
     * operations are delivered in the same order as they are originally written in HBase (which
     * will always be the case except for when a region split or move takes place), this metric will always
     * hold the write timestamp of the most recent operation in HBase that has been handled by the SEP system.
     * 
     * @param writeTimestamp The write timestamp of the last SEP operation
     */
    public void reportSepTimestamp(long writeTimestamp) {
        lastTimestampInputProcessed.set(writeTimestamp);
    }

    @Override
    public void doUpdates(MetricsContext unused) {
        synchronized (this) {
            for (MetricsBase m : metricsRegistry.getMetricsList()) {
                m.pushMetric(metricsRecord);
            }
        }
        metricsRecord.update();
    }

    public class SepMetricsMXBean extends MetricsDynamicMBeanBase {
        private final ObjectName mbeanName;

        public SepMetricsMXBean(MetricsRegistry registry) {
            super(registry, "HBase Side-Effect Processor Metrics");

            mbeanName = MBeanUtil.registerMBean("SEP", recordName, this);
        }

        public void shutdown() {
            if (mbeanName != null) {
                MBeanUtil.unregisterMBean(mbeanName);
            }
        }
    }

}
