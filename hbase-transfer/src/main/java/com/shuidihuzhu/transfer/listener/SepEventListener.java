package com.shuidihuzhu.transfer.listener;

import com.google.common.collect.Maps;
import com.shuidihuzhu.sep.EventListener;
import com.shuidihuzhu.sep.SepEvent;
import com.shuidihuzhu.transfer.model.SinkRecord;
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

    @Value("${hbase-transfer.hbase.table}")
    private String tableName;

    @Override
    public void processEvents(List<SepEvent> sepEvents) {
        for (SepEvent sepEvent : sepEvents) {
            String table = Bytes.toString(sepEvent.getTable());
            if(!table.equals(tableName)){
                continue;
            }
            String payload = Bytes.toString(sepEvent.getPayload());
            SinkRecord record = new SinkRecord();

            Map<String, Object> keyValues = Maps.newHashMap();
            String rowKey = null;
            String id = null;
            boolean isErrRowKey=false;
            for (Cell cell : sepEvent.getKeyValues()) {
                rowKey = Bytes.toString(CellUtil.cloneRow(cell));

                if(rowKey.contains("-") || rowKey.contains(":")==false || rowKey.split(":").length < 2){
                    logger.warn("Discarded --- rowKey=" + rowKey);
                    isErrRowKey = true;
                    break;
                }

                id = rowKey.split(":")[1];

                try {
                    Long.parseLong(id);
                }catch (NumberFormatException e){
                    logger.warn("Discarded --- rowKey=" + rowKey + ", not long type : " + id);
                    isErrRowKey = true;
                    break;
                }

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

                if(column.contains(".")){
                    column = column.replace(".","_");
                }

                record.setColumn(column);
                keyValues.put(column, value);
            }

            if(isErrRowKey){
                continue;
            }

            keyValues.put("id",id);
            record.setKeyValues(keyValues);

            try {
                kafkaSink.sink(record);
            } catch (Exception e) {
                logger.error("kafka sink error.",e);
                System.err.println("kafka error=" + SinkRecord.getText(record));
            }
        }
    }



}
