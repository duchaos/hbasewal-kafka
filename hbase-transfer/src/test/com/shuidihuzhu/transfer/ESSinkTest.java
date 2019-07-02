package com.shuidihuzhu.transfer;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.sink.elasticsearch.ESSink;
import io.searchbox.core.Bulk;
import org.junit.Test;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @author duchao
 * @version : com.shuidihuzhu.transfer.sink.elasticsearch.com.shuidihuzhu.transfer.ESSinkTest.java, v 0.1 2019-06-22 15:46 duchao Exp $
 */

public class ESSinkTest extends TestBase{

    @Resource
    private ESSink userInfoESSink;

    @Resource
    private ESSink deviceInfoESSink;

    @Test
    public void batchUpdateAction() throws Exception {
        List<SinkRecord> list =new ArrayList<>();
//        HashMap<String, String > userMap = new HashMap<>();
//        userMap.put("id","2699999999");
//        userMap.put("name","duchao");
//        userMap.put("ip","lllll");
//        map.put("user",userMap);
        SinkRecord recode = new SinkRecord();
        Map<String, Object> map = new HashMap<>(3);
        map.put("data_device_id", "FE1CB01E-CE25-40I44-A2E1YYY");
        map.put("data_basic_ssss", "sasdasdfa");
        map.put("id","719471962");
        recode.setKeyValues(map);
        recode.setRowKey("this is rowkey");
        list.add(recode);
        userInfoESSink.batchSink(list);
    }

    @Test
    public void searchDocumen() throws Exception {
        SinkRecord record = new SinkRecord();
        record.setKeyValues(new HashMap(){
            {
                put("id","719471962");
            }
        });
        SinkRecord record1 = new SinkRecord();
        record1.setKeyValues(new HashMap(){
            {
                put("id","352438581");
            }
        });
        ArrayList<SinkRecord> arrayList = Lists.newArrayList(record, record1);
        List<SinkRecord> sinkRecords = userInfoESSink.doQuery_FromES("sdhz_user_info_realtime", "detail", arrayList, arrayList.size());
        System.out.println(JSON.toJSONString(sinkRecords));

    }

    @Test
    public void batchInsertAction() throws Exception {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex("sdhz_device_info_realtime").defaultType("detail");
        SinkRecord recode = new SinkRecord();
        Map<String, Object> map = new HashMap<>(8);
        map.put("data_dev_user_id", "719471962");
        map.put("data_aaaa", "123123121");
        map.put("id", "FE1CB01E-CE25-40I44-A2E1YYY");
        recode.setKeyValues(map);
        recode.setRowKey("this is rowkey");
        List<SinkRecord> list = new ArrayList<>();
        list.add(recode);
        deviceInfoESSink.batchUpdateAction(list,bulkBuilder);
    }

}