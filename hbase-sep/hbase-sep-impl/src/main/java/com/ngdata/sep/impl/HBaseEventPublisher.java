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

import com.ngdata.sep.EventPublisher;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

/**
 * Publishes side-effect payload data directly to an HBase table, for distribution via the SEP.
 */
public class HBaseEventPublisher implements EventPublisher {

    private final Table payloadTable;
    private final byte[] payloadColumnFamily;
    private final byte[] payloadColumnQualifier;

    /**
     * Construct with the table and column information to which payload data will be written.
     *
     * @param payloadTable table where payload data will be written
     * @param payloadColumnFamily column family where payload data will be written
     * @param payloadColumnQualifier column qualifier under which payload data will be written
     */
    public HBaseEventPublisher(Table payloadTable, byte[] payloadColumnFamily, byte[] payloadColumnQualifier) {
        this.payloadTable = payloadTable;
        this.payloadColumnFamily = payloadColumnFamily;
        this.payloadColumnQualifier = payloadColumnQualifier;
    }

    @Override
    public void publishEvent(byte[] row, byte[] payload) throws IOException {
        Put eventPut = new Put(row);
        eventPut.addColumn(payloadColumnFamily, payloadColumnQualifier, payload);
        payloadTable.put(eventPut);
    }

    protected Table getPayloadTable() {
        return this.payloadTable;
    }
}
