package com.shuidihuzhu.transfer.sink;

import com.shuidihuzhu.transfer.model.SinkRecord;

import java.util.List;

/**
 * Created by sunfu on 2018/12/29.
 */
public abstract class AbstractSink {

    public  void sink(String topic, SinkRecord record){};

    public  void batchSink(List<SinkRecord> records){};

    public void handleErrorRecord(SinkRecord record) {
        throw new RuntimeException("transfer error");
    }

    public void handleBatchErrorRecord(List<SinkRecord> records) {
        for (SinkRecord record : records) {
            handleErrorRecord(record);
        }
    }
}
