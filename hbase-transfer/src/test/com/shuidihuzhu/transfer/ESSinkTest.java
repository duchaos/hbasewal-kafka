package com.shuidihuzhu.transfer;

import com.alibaba.fastjson.JSON;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.sink.elasticsearch.ESSink;
import io.searchbox.client.JestResult;
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
        map.put("data_device_id", "866488039800000");
        map.put("data_basic_idcard_city", "呼和浩特");
        map.put("id","352436579");
        recode.setKeyValues(map);
        recode.setRowKey("this is rowkey");
        list.add(recode);
        userInfoESSink.batchSink(list);
    }

    @Test
    public void batchInsertAction() throws Exception {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex("sdhz_device_info_realtime").defaultType("detail");
        SinkRecord recode = new SinkRecord();
        Map<String, Object> map = new HashMap<>(8);
        map.put("data_dev_device_type", "荣耀 大师答玩");
        map.put("data_dev_user_id", "719471962");
        map.put("id", "FE1CB01E-CE25-40C3-A2E1YYY");
//        map.put("device_id","2699999999");
        Map<String, Object> usermap = new HashMap<>(3);
        usermap.put("id","767761440");
        usermap.put("data_basic_idcard_name","上海");
//        map.put(USER,usermap);
        recode.setKeyValues(map);
        recode.setRowKey("this is rowkey");
        List<SinkRecord> list = new ArrayList<>();
        list.add(recode);
        deviceInfoESSink.batchUpdateAction(list,bulkBuilder);
    }

    @Test
    public void searchDocumentById() throws Exception {
        JestResult jestResult = userInfoESSink.searchDocumentById("sdhz_user_info_realtime", "detail", "719471962");
        if (jestResult.isSucceeded()){
            Map sourceAsObject = jestResult.getSourceAsObject(Map.class);

            System.out.println(JSON.toJSONString(sourceAsObject));

        }


    }
}