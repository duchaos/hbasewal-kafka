package com.shuidihuzhu.transfer.listener;

import com.google.common.collect.Maps;
import com.shuidihuzhu.sep.EventListener;
import com.shuidihuzhu.sep.SepEvent;
import com.shuidihuzhu.transfer.model.Config;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.sink.ESSink;
import com.shuidihuzhu.transfer.sink.KafkaSink;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by sunfu on 2018/12/29.
 */
@Service
public class SepEventListener implements EventListener {

    private Logger logger = LoggerFactory.getLogger(SepEventListener.class);

    @Autowired
    private KafkaSink kafkaSink;
    @Autowired
    private ESSink eSSink;

    @Value("${hbase-transfer.hbase.table}")
    private String tableName;

    @Override
    public void processEvents(List<SepEvent> sepEvents) {
//        logger.debug("Config.openKafka={}",Config.openKafka);
//        logger.debug("Config.openEs={}",Config.openEs);
        for (SepEvent sepEvent : sepEvents) {
            String table = Bytes.toString(sepEvent.getTable());
            if(!table.equals(tableName)){
                continue;
            }
            String payload = Bytes.toString(sepEvent.getPayload());
            SinkRecord record = new SinkRecord();

            Map<String, Object> keyValues = Maps.newHashMap();
            for (Cell cell : sepEvent.getKeyValues()) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                record.setTable(table);
                record.setFamily(family);
                record.setQualifier(qualifier);
                record.setRowKey(rowKey);
                record.setTimestamp(timestamp);
                record.setValue(value);
                record.setPayload(payload);

                String column = "";
                if (family.equals("data")) {
                    column = family + "_" + qualifier;
                } else {
                    column = qualifier;
                }
                record.setColumn(column);
                keyValues.put(column, value);
            }

            record.setKeyValues(keyValues);

            if(Config.openKafka){
                try {
                    kafkaSink.sink(record);
                } catch (Exception e) {
                    logger.error("kafka sink error.",e);
                    System.err.println("kafka error=" + SinkRecord.getText(record));
                }
            }

            if(Config.openEs){
                try {
                    eSSink.sink(record);
                } catch (Exception e) {
                    logger.error("es sink error.",e);
                    System.err.println("es error=" + SinkRecord.getText(record));
                }
            }

        }
    }



}
