package com.ngdata.wal.sink;

import com.ngdata.wal.model.SinkRecord;

public abstract class AbstractSink {

    public  void sink(String topic, SinkRecord record){};


    public void handleErrorRecord(SinkRecord record) {
        throw new RuntimeException("transfer error");
    }
}
