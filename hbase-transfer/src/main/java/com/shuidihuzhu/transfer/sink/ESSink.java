package com.shuidihuzhu.transfer.sink;

import com.shuidihuzhu.transfer.model.SinkRecord;
import org.apache.hadoop.hbase.Cell;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by sunfu on 2018/12/29.
 */
@Service
public class ESSink extends AbstractSink {

    @Override
    public void sink(SinkRecord record) {

    }

    @Override
    public void batchSink(List<SinkRecord> records) {

    }
}
