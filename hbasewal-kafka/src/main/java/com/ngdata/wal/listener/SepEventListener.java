package com.ngdata.wal.listener;

import com.google.common.collect.Maps;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.SepEvent;
import com.ngdata.wal.model.SinkRecord;
import com.ngdata.wal.service.KafkaProducerService;
import com.ngdata.wal.sink.Sink;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @author duchao
 */
@Slf4j
@Service
public class SepEventListener implements EventListener, ApplicationContextAware {

    private final KafkaProducerService kafkaProducerService;


    public SepEventListener(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }
    private Map<String, Sink> HBaseSinkMap = Maps.newConcurrentMap();

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, Sink> sinkMap = applicationContext.getBeansOfType(Sink.class);
        if (MapUtils.isEmpty(sinkMap)) {
            log.error("没有配置任何 WAL 同步类！");
        }
        for (Sink sink : sinkMap.values()) {
            log.info("容器加载WAL 同步 表[{}] ,类名", sink.getTableName(), sink.getClass().getSimpleName());
            HBaseSinkMap.put(sink.getTableName(), sink);
        }
    }

    /**
     * 消费WAL过来的数据
     * 需要开启表的REPLICATION_SCOPE属性
     * alter 'table',{NAME => 'xxx',REPLICATION_SCOPE => '1'}
     *
     * @param sepEvents
     * @date: 2019-06-20 21:32:43
     */
    @Override
    public void processEvents(List<SepEvent> sepEvents) {
        for (SepEvent sepEvent : sepEvents) {
            SinkRecord sinkRecord = parseEvent(sepEvent);
            if (sinkRecord == null) {
                log.error("sinkRecord is null");
                continue;
            }
            String table = sinkRecord.getTable();
            if (table == null) {
                log.error("sinkRecord table is null: " + sinkRecord);
                continue;
            }
            kafkaProducerService.send(sinkRecord, HBaseSinkMap.get(table).parse(sinkRecord));
        }
    }

    private SinkRecord parseEvent(SepEvent sepEvent) {
        SinkRecord record = new SinkRecord();
        String hBaseTable = Bytes.toString(sepEvent.getTable());
        record.setTable(hBaseTable);
        String rowKey = Bytes.toString(sepEvent.getRow());
        record.setRowKey(rowKey);
        Map<String, Object> keyValues = Maps.newHashMap();
        for (Cell cell : sepEvent.getKeyValues()) {
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            keyValues.put(family + ":" + qualifier, value);
        }
        Cell firstCell = sepEvent.getKeyValues().get(0);
        long timestamp = firstCell.getTimestamp();
        record.setTimestamp(timestamp);
        boolean delete = CellUtil.isDelete(firstCell);
        record.setDelete(delete);
        record.setKeyValues(keyValues);
        return record;
    }


}
