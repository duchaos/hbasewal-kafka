package com.shuidihuzhu.transfer.listener;

import com.google.common.collect.Maps;
import com.shuidihuzhu.sep.EventListener;
import com.shuidihuzhu.sep.SepEvent;
import com.shuidihuzhu.transfer.enums.TransferEnum;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.service.SepEventTranslator;
import com.shuidihuzhu.transfer.sink.kafka.KafkaSink;
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
    private final ThreadLocal<SepEventTranslator> threadLocalTranslator = new ThreadLocal<>();


    @Autowired
    private KafkaSink kafkaSink;

    @Value("${hbase-transfer.hbase.table}")
    private String tableName;

    /**
     * 消费WAL过来的数据
     * 需要开启表的REPLICATION_SCOPE属性
     * {@code alter 'sdhz_device_info_realtime',{NAME => 'data',REPLICATION_SCOPE => '1'}}
     *
     * @param sepEvents
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-06-20 21:32:43
     */
    @Override
    public void processEvents(List<SepEvent> sepEvents) {

        for (SepEvent sepEvent : sepEvents) {

            String table = Bytes.toString(sepEvent.getTable());
            if (!table.equals(tableName)) {
                continue;
            }
            String payload = Bytes.toString(sepEvent.getPayload());
            SinkRecord record = new SinkRecord();

            Map<String, Object> keyValues = Maps.newHashMap();
            String rowKey = null;
            String id = null;
            boolean isErrRowKey = false;
            for (Cell cell : sepEvent.getKeyValues()) {
                rowKey = Bytes.toString(CellUtil.cloneRow(cell));

                if (rowKey.contains("-") || rowKey.contains(":") == false || rowKey.split(":").length < 2) {
                    logger.warn("Discarded --- rowKey=" + rowKey);
                    isErrRowKey = true;
                    break;
                }

                id = rowKey.split(":")[1];

                try {
                    Long.parseLong(id);
                } catch (NumberFormatException e) {
                    logger.warn("Discarded --- rowKey=" + rowKey + ", not long type : " + id);
                    isErrRowKey = true;
                    break;
                }

                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
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

                if (column.contains(".")) {
                    column = column.replace(".", "_");
                }

                record.setColumn(column);
                keyValues.put(column, value);
            }

            if (isErrRowKey) {
                continue;
            }

            keyValues.put("id", id);
            record.setKeyValues(keyValues);

            try {
                kafkaSink.sink(record);
            } catch (Exception e) {
                logger.error("kafka sink error.", e);
                System.err.println("kafka error=" + SinkRecord.getText(record));
            }

//        TODO 降级方案

            try {
                publishMessage(sepEvent);
            } catch (Exception e) {
                logger.error("kafka sink error.", e);
            }
        }
    }

    private void publishMessage(SepEvent sepEvent) {
        logWithThreadLocalTranslator(kafkaSink, sepEvent);
    }


    private void logWithThreadLocalTranslator(KafkaSink kafkaSink, SepEvent sepEvent) {
        // Implementation note: this method is tuned for performance. MODIFY WITH CARE!
        final SepEventTranslator translator = getCachedTranslator();
        initTranslator(translator, sepEvent);
        publish(kafkaSink, translator);
    }


    private SepEventTranslator getCachedTranslator() {
        SepEventTranslator result = threadLocalTranslator.get();
        if (result == null) {
            result = new SepEventTranslator();
            threadLocalTranslator.set(result);
        }
        return result;
    }

    private void initTranslator(final SepEventTranslator translator, final SepEvent sepEvent) {
//默认设置事件为有效事件 status 为 true
        translator.setBasicValues(sepEvent);
    }

    private void publish(KafkaSink kafkaSink, final SepEventTranslator translator) {
        SinkRecord sinkRecord = new SinkRecord();
        TransferEnum transferEnum = translator.translateTo(sinkRecord);
        kafkaSink.sink(transferEnum.getTopic(), sinkRecord);
    }

}
