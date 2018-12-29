package com.shuidihuzhu.transfer.sink;

import com.shuidihuzhu.transfer.model.SinkRecord;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by sunfu on 2018/12/29.
 */
@Service
public class KafkaSink extends AbstractSink {

    @Override
    public void sink(SinkRecord record) {

        try {

        } catch (Exception e) {
            handleErrorRecord(record);
        }

    }

    @Override
    public void batchSink(List<SinkRecord> records) {

    }
}
