package com.shuidihuzhu.transfer;

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

import static org.apache.hadoop.yarn.webapp.Params.USER;


/**
 * @author duchao
 * @version : com.shuidihuzhu.transfer.sink.elasticsearch.com.shuidihuzhu.transfer.ESSinkTest.java, v 0.1 2019-06-22 15:46 duchao Exp $
 */

public class ESSinkTest {

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
        map.put("data_basic_wx_nickname", "啦啦");
        map.put("data_basic_wx_province", "四川");
        map.put("id","7888888");
        map.put("device_id","26999998000");
        recode.setKeyValues(map);
        recode.setRowKey("this is rowkey");
        list.add(recode);
        userInfoESSink.batchSink(list);
    }

    @Test
    public void batchInsertAction() throws Exception {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex("sdhz_device_info_realtime").defaultType("detail");
        Map<String, SinkRecord> maps = new HashMap<>();
        SinkRecord recode = new SinkRecord();
//        Map<String, Object> map = new HashMap<>(2);
//        map.put("data_basic_wx_province", "上天");
//        map.put("data_basic_wx_nickname", "三大爷");
//        map.put("id","2699999999");
//        recode.setKeyValues(map);
//        maps.put("2699999999",recode);
        Map<String, Object> map = new HashMap<>(3);
        map.put("os", "IOS");
        map.put("NET","4G");
        map.put("id","26999998000");
//        map.put("device_id","2699999999");
        Map<String, Object> usermap = new HashMap<>(3);
        usermap.put("id","7888888");
        usermap.put("data_basic_wx_nickname","北京");
        map.put(USER,usermap);
        recode.setKeyValues(map);
        recode.setRowKey("this is rowkey");
        List<SinkRecord> list = new ArrayList<>();
        list.add(recode);
        deviceInfoESSink.batchUpdateAction(list,bulkBuilder);
    }

    @Test
    public void searchDocumentById() throws Exception {
        JestResult jestResult = userInfoESSink.searchDocumentById("sdhz_user_info_realtime", "detail", "269381854");

    }
}