package com.shuidihuzhu.transfer.sink;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

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

        String[] eserverUrls = esUrl.split(",");
        Set<String> serverList = new LinkedHashSet<String>();
        for(String url : eserverUrls){
            serverList.add(url);
            logger.info("init Es url ===> " + url);
        }

        JestClientFactory factory = new JestClientFactory();

        String ELASTIC_SEARCH_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        Gson gson = new GsonBuilder()
                .setDateFormat(ELASTIC_SEARCH_DATE_FORMAT)
                .create();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(serverList)
                .defaultCredentials(username,password)
                .multiThreaded(true)
                .defaultMaxTotalConnectionPerRoute(1)
                .connTimeout(10000)
                .readTimeout(1200000)
                .gson(gson)
                .maxTotalConnection(12).build());

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

    private JestResult batchInsertAction(Map<String,SinkRecord> recordMap) throws Exception {
        System.out.println("batchInsertAction ===> start...  " + recordMap.size());

        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
        Index index = null;
        Map<String, String> idAndRowKeyMap = new HashMap();
        for (SinkRecord record : recordMap.values()) {
            String id = String.valueOf(record.getKeyValues().get("id"));
            if (!StringUtils.isEmpty(id)) {
//                    System.out.println("Insert rowkey = " + record.getRowKey() + ", column num: " + (record.getKeyValues().size() -1));
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
            Map<String,SinkRecord> rejectedRecordMap = new HashMap<>();
            for(BulkResult.BulkResultItem item : errItems){
                String id = item.id;
                if (item.status == 429 || item.status == 503 || item.status == 500) {
                    //拒绝，node异常，重新执行
                    rejectedRecordMap.put(id,recordMap.get(id));
                }else {
                    String logInfo = "batchInsertAction ===> Hbase rowkey=" + idAndRowKeyMap.get(item.id) + " Error item = [id : " + item.id + ", index : " + item.index + "type : " + item.type + ",operation :" + item.operation + ",status :" + item.status + ",version :" + item.version + ",error :" + item.error + ",errorType :" + item.errorType + ",errorReason :" + item.errorReason + "]";
                    logger.error(logInfo);
                }
            }

            if(rejectedRecordMap.isEmpty() == false){
                result = batchInsertAction(rejectedRecordMap);
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
            Map<String,SinkRecord> insertRecordMap = new HashMap();
            for(BulkResult.BulkResultItem item : errItems) {

                String id = item.id;
                if (item.status == 404 || item.status == 429  || item.status == 503  || item.status == 500 || item.status == 409   ) {
                    //索引不存在,拒绝，node异常，重新执行
                    insertRecordMap.put(id, recordMap.get(id));
                }else {
                    String logInfo = "batchUpdateAction ===> Hbase rowkey=" + recordMap.get(id).getRowKey() + " Error item = [id : " + item.id + ", index : " + item.index + "type : " + item.type + ",operation :" + item.operation + ",status :" + item.status + ",version :" + item.version + ",error :" + item.error + ",errorType :" + item.errorType + ",errorReason :" + item.errorReason + "]";
                    logger.error(logInfo);
                }
            }

            if (!insertRecordMap.isEmpty()) {
                result = batchInsertAction(insertRecordMap);

                if (!result.isSucceeded()) {
                    throw new Exception("execute es error.msg=" + result.getErrorMessage());
                }
            }
        }

        return result;
    }
}
