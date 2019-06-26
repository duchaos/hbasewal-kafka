package com.shuidihuzhu.transfer.listener;

import com.shuidihuzhu.sep.EventListener;
import com.shuidihuzhu.sep.SepEvent;
import com.shuidihuzhu.transfer.enums.TransferEnum;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.service.SepEventTranslator;
import com.shuidihuzhu.transfer.sink.kafka.KafkaSink;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

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
     * {@code alter 'sdhz_device_info_realtime',{NAME => 'Client',REPLICATION_SCOPE => '1'}}
     *
     * @param sepEvents
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-06-20 21:32:43
     */
    @Override
    public void processEvents(List<SepEvent> sepEvents) {

        for (SepEvent sepEvent : sepEvents) {

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
        if (null == transferEnum || StringUtils.isBlank(transferEnum.getTopic())){
            return;
        }
        kafkaSink.sink(transferEnum.getTopic(),sinkRecord);
    }

}
