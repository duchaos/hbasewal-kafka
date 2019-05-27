package com.shuidihuzhu.transfer.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by sunfu on 2018/12/29.
 */
@Service
public class ESSink extends AbstractSink implements InitializingBean {

    private Logger logger = LoggerFactory.getLogger(ESSink.class);

    private JestClient client;

    @Value("${hbase-transfer.elasticsearch.index}")
    private String indexName;
    @Value("${hbase-transfer.elasticsearch.type}")
    private String indexType;
    @Value("${hbase-transfer.elasticsearch.url}")
    private String esUrl;
    @Value("${hbase-transfer.elasticsearch.username}")
    private String username;
    @Value("${hbase-transfer.elasticsearch.password}")
    private String password;


    @Override
    public void afterPropertiesSet() throws Exception {
        JestClientFactory factory = new JestClientFactory();

        String ELASTIC_SEARCH_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        Gson gson = new GsonBuilder()
                .setDateFormat(ELASTIC_SEARCH_DATE_FORMAT)
                .create();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(esUrl)
                .defaultCredentials(username,password)
                .multiThreaded(true)
                .defaultMaxTotalConnectionPerRoute(2)
                .connTimeout(3600000)
                .readTimeout(3600000)
                .gson(gson)
                .maxTotalConnection(10).build());

        client = factory.getObject();

        System.err.println("indexName = " + this.indexName);
        //TODO: Test38
//        indexName = "sdhz_user_info_realtime_table3";

        //TODO: online
        indexName = "sdhz_user_info_realtime_table8";
    }

    @Override
    public void batchSink(List<SinkRecord> recordList) {
        try{
            batchUpdateAction(recordList);
        }catch (Exception e){
            logger.error("into es error.",e);
            handleBatchErrorRecord(recordList);
        }
    }

    private JestResult batchInsertAction(List<SinkRecord> recordList) throws Exception {
//        System.out.println("batchInsertAction ===> start...  " + recordList.size());

        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
        Index index = null;
        Map<String, String> idAndRowKeyMap = new HashMap();
        for (SinkRecord record : recordList) {
            String id = (String) record.getKeyValues().get("id");
            if (!StringUtils.isEmpty(id)) {
                logger.info("Insert rowkey = " + record.getRowKey() + ", column num: " + (record.getKeyValues().size() -1));
                idAndRowKeyMap.put(id,record.getRowKey());

                index = new Index.Builder(record.getKeyValues()).id(id).build();
            } else {
                index = new Index.Builder(record.getKeyValues()).build();
            }
            bulkBuilder.addAction(index).build();
        }

        JestResult result = client.execute(bulkBuilder.build());
        if (!result.isSucceeded()) {
            List<BulkResult.BulkResultItem> errItems = ((BulkResult) result).getFailedItems();
            for(BulkResult.BulkResultItem item : errItems){
                String logInfo = "batchInsertAction ===> Hbase rowkey=" + idAndRowKeyMap.get(item.id) +" Error item = [id : "  + item.id + ", index : " + item.index + "type : " + item.type + ",operation :" + item.operation + ",status :"  + item.status + ",version :"  + item.version + ",error :"   + item.error + ",errorType :" + item.errorType + ",errorReason :"+ item.errorReason  + "]";
                logger.error(logInfo);
            }
        }
        return result;
    }

    public JestResult batchUpdateAction(List<SinkRecord> recordList) throws Exception{
        Bulk.Builder bulkBuilder =new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
        Update update = null;

        Map<String,SinkRecord> recordMap = new HashMap();
        for(SinkRecord record : recordList){
            Object idObj = record.getKeyValues().get("id");
            String id = null;
            if(idObj != null ){
                id = String.valueOf(idObj);
            }else{
                throw new Exception("rowkey is null");
            }

            if( !StringUtils.isEmpty(id)){
//                logger.info("Update rowkey = " + record.getRowKey() + ", column num: " + (record.getKeyValues().size() -1));
                Map docMap = Maps.newHashMap();
                docMap.put("doc",record.getKeyValues());
                update = new Update.Builder(JSON.toJSONString(docMap)).id(id).setParameter("retry_on_conflict",5).build();
            }else{
                throw new Exception("rowkey is null");
            }
            bulkBuilder.addAction(update).build();

            if (recordMap.containsKey(id)) {
                recordMap.get(id).getKeyValues().putAll(record.getKeyValues());
            } else {
                recordMap.put(id, record);
            }
        }
        JestResult result = client.execute(bulkBuilder.build());
        if (!result.isSucceeded()) {
            List<BulkResult.BulkResultItem> errItems = ((BulkResult) result).getFailedItems();
            List<SinkRecord> insertRecordList = new ArrayList();
            Set<String> idSet = new HashSet();
            for(BulkResult.BulkResultItem item : errItems) {

                String id = item.id;
                if (item.status == 404) {
                    if(idSet.contains(id) == false) {
                        //索引不存在,就插入
                        insertRecordList.add(recordMap.get(id));
                        idSet.add(id);
                    }
                }else {
                    String logInfo = "batchUpdateAction ===> Hbase rowkey=" + recordMap.get(id).getRowKey() + " Error item = [id : " + item.id + ", index : " + item.index + "type : " + item.type + ",operation :" + item.operation + ",status :" + item.status + ",version :" + item.version + ",error :" + item.error + ",errorType :" + item.errorType + ",errorReason :" + item.errorReason + "]";
                    logger.error(logInfo);
                }
            }

            if (!insertRecordList.isEmpty()) {
                result = batchInsertAction(insertRecordList);

                if (!result.isSucceeded()) {
                    throw new Exception("execute es error.msg=" + result.getErrorMessage());
                }
            }
        }

        return result;
    }
}
