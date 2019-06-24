package com.shuidihuzhu.transfer.sink.kafka;

import com.shuidihuzhu.transfer.TestBase;
import com.shuidihuzhu.transfer.model.SinkRecord;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashMap;
import java.util.Map;

/**
 * @author duchao
 * @version : KafkaSinkTest.java, v 0.1 2019-06-24 16:54 duchao Exp $
 */
public class KafkaSinkTest extends TestBase {

    @Autowired
    private KafkaSink kafkaSink;

    @Test
    public void sink() {
        SinkRecord recode = new SinkRecord();
        Map<String, Object> map = new HashMap<>(3);
        map.put("data_basic_wx_nickname", "啦啦");
        map.put("data_basic_wx_province", "四川");
        map.put("id","7888888");
        map.put("device_id","26999998000");
        recode.setKeyValues(map);
        recode.setRowKey("this is rowkey");
        kafkaSink.sink("hbase-tranfer",recode);
    }

    @Test
    public void sink1() {
    }
}