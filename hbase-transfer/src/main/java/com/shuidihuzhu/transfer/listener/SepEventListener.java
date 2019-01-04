package com.shuidihuzhu.transfer.listener;

import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.sink.ESSink;
import com.shuidihuzhu.transfer.sink.KafkaSink;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by sunfu on 2018/12/29.
 */
@Service
public class SepEventListener implements EventListener {

    @Autowired
    KafkaSink kafkaSink;
    @Autowired
    ESSink eSSink;

    @Override
    public void processEvents(List<SepEvent> sepEvents) {
        for (SepEvent sepEvent : sepEvents) {
            String table = Bytes.toString(sepEvent.getTable());
            String payload = Bytes.toString(sepEvent.getPayload());
            for (Cell cell : sepEvent.getKeyValues()) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                SinkRecord record = new SinkRecord();
                record.setTable(table);
                record.setFamily(family);
                record.setQualifier(qualifier);
                record.setRowKey(rowKey);
                record.setTimestamp(timestamp);
                record.setValue(value);
                record.setPayload(payload);
                System.out.println(record.toString());

                //kafkaSink.sink(record);
                eSSink.sink(record);
            }
        }
    }



}
