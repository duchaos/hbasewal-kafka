package com.shuidihuzhu.transfer.sink.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.google.gson.*;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.sink.AbstractSink;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.*;

import static com.shuidihuzhu.transfer.sink.elasticsearch.DeviceInfoESSink.SEARCH_USER_ID;
import static com.shuidihuzhu.transfer.sink.elasticsearch.DeviceInfoESSink.USER_ID;
import static org.apache.hadoop.yarn.webapp.Params.USER;


/**
 * Created by sunfu on 2018/12/29.
 */
public class ESSink extends AbstractSink implements InitializingBean {

    public Logger logger = LoggerFactory.getLogger(ESSink.class);

    public JestClient client;

    @Value("${hbase-transfer.elasticsearch.index}")
    public String indexName;
    @Value("${hbase-transfer.elasticsearch.type}")
    public String indexType;
    @Value("${hbase-transfer.elasticsearch.url}")
    public String esUrl;
    @Value("${hbase-transfer.elasticsearch.username}")
    public String username;
    @Value("${hbase-transfer.elasticsearch.password}")
    public String password;


    @Override
    public void afterPropertiesSet() throws Exception {

        String[] eserverUrls = esUrl.split(",");
        Set<String> serverList = new LinkedHashSet<String>();
        for (String url : eserverUrls) {
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
                .defaultCredentials(username, password)
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
//        indexName = "sdhz_user_info_realtime_table8";
    }

    @Override
    public void batchSink(List<SinkRecord> recordList) {
        try {
            batchUpdateAction(recordList);
        } catch (Exception e) {
            logger.error("into es error.", e);
            handleBatchErrorRecord(recordList);
        }
    }

    /**
     * @param recordList 需要处理的数据
     * @return {@link JestResult }
     * @throw
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-06-21 13:22:43
     */
    private JestResult batchUpdateAction(List<SinkRecord> recordList) throws Exception {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
        Map<String, SinkRecord> recordMap = recordPreUpdate(recordList, bulkBuilder);
        JestResult result = client.execute(bulkBuilder.build());
        result = afterUpdateProcess(recordMap, result);
        return result;
    }

    /**
     * @param recordList  需要处理的数据
     * @param bulkBuilder es bulkBuilder
     * @return {@link JestResult }
     * @throw
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-06-21 13:22:43
     */
    public JestResult batchUpdateAction(List<SinkRecord> recordList, Bulk.Builder bulkBuilder) throws Exception {
        Map<String, SinkRecord> recordMap = recordPreUpdate(recordList, bulkBuilder);
        JestResult result = client.execute(bulkBuilder.build());
        result = afterUpdateProcess(recordMap, result);
        return result;
    }

    public JestResult batchInsertAction(Map<String, SinkRecord> recordMap, Bulk.Builder bulkBuilder) throws Exception {
        Map<String, String> idAndRowKeyMap = recordPreInsert(recordMap, bulkBuilder);
        JestResult result = client.execute(bulkBuilder.build());
        result = afterInsertProcess(recordMap, idAndRowKeyMap, result);
        return result;
    }


    public Map<String, String> recordPreInsert(Map<String, SinkRecord> recordMap, Bulk.Builder bulkBuilder) {
        Index index = null;
        Map<String, String> idAndRowKeyMap = new HashMap();
        if (MapUtils.isEmpty(recordMap)) {
            return idAndRowKeyMap;
        }
        for (SinkRecord record : recordMap.values()) {
            if (record == null) {
                continue;
            }
            Map<String, Object> keyValues = record.getKeyValues();
            if (MapUtils.isEmpty(keyValues)) {
                continue;
            }
            String id = String.valueOf(keyValues.get("id"));
            if (!StringUtils.isEmpty(id)) {
//                    System.out.println("Insert rowkey = " + record.getRowKey() + ", column num: " + (record.getKeyValues().size() -1));
                idAndRowKeyMap.put(id, record.getRowKey());

                index = new Index.Builder(keyValues).id(id).build();
            } else {
                index = new Index.Builder(keyValues).build();
            }
            bulkBuilder.addAction(index).build();
        }
        return idAndRowKeyMap;
    }


    public Map<String, SinkRecord> recordPreUpdate(List<SinkRecord> recordList, Bulk.Builder bulkBuilder) throws Exception {
        Update update = null;
        Map<String, SinkRecord> recordMap = new HashMap();
        if (CollectionUtils.isEmpty(recordList)) {
            logger.error("ESSink.recordPreUpdate recordList is empty!");
            return recordMap;
        }
        for (SinkRecord record : recordList) {
            Map<String, Object> updateMap = record.getKeyValues();
            if (MapUtils.isEmpty(updateMap)) {
                continue;
            }
//           这里的id ，可以是 用户画像的id ，也可以是 设备画像的id
//              id 的作用，就是 更新索引时候，锁定更新的document
            Object idObj = updateMap.get("id");
            String id;
            if (idObj != null) {
                id = String.valueOf(idObj);
            } else {
                throw new Exception("rowkey is null");
            }

            if (!StringUtils.isEmpty(id)) {
                Map<String, Map<String, Object>> docMap = new HashMap<>(1);
                docMap.put("doc", updateMap);
                id = String.valueOf(updateMap.get("id"));
                update = new Update.Builder(JSON.toJSONString(docMap)).id(id).build();
            } else {
                throw new Exception("rowkey is null");
            }
            bulkBuilder.addAction(update).build();
            if (recordMap.containsKey(id)) {
                recordMap.get(id).getKeyValues().putAll(updateMap);
            } else {
                recordMap.put(id, record);
            }
        }
        return recordMap;
    }


    public JestResult afterInsertProcess(Map<String, SinkRecord> recordMap, Map<String, String> idAndRowKeyMap, JestResult result) throws Exception {
        if (!result.isSucceeded()) {
            List<BulkResult.BulkResultItem> errItems = ((BulkResult) result).getFailedItems();
            Map<String, SinkRecord> rejectedRecordMap = new HashMap<>();
            for (BulkResult.BulkResultItem item : errItems) {
                String id = item.id;
                if (item.status == 429 || item.status == 503 || item.status == 500) {
                    //拒绝，node异常，重新执行
                    rejectedRecordMap.put(id, recordMap.get(id));
                } else {
                    String logInfo = "batchInsertAction ===> Hbase rowkey=" + idAndRowKeyMap.get(item.id) + " Error item = [id : " + item.id + ", index : " + item.index + "type : " + item.type + ",operation :" + item.operation + ",status :" + item.status + ",version :" + item.version + ",error :" + item.error + ",errorType :" + item.errorType + ",errorReason :" + item.errorReason + "]";
                    logger.error(logInfo);
                }
            }

            if (!rejectedRecordMap.isEmpty()) {
                result = batchInsertAction(rejectedRecordMap);
            }
        }
        return result;
    }

    public JestResult batchInsertAction(Map<String, SinkRecord> rejectedRecordMap) throws Exception {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
        return batchInsertAction(rejectedRecordMap, bulkBuilder);
    }

    public JestResult afterUpdateProcess(Map<String, SinkRecord> recordMap, JestResult result) throws Exception {
        if (!result.isSucceeded()) {
            List<BulkResult.BulkResultItem> errItems = ((BulkResult) result).getFailedItems();
            Map<String, SinkRecord> insertRecordMap = new HashMap();
            for (BulkResult.BulkResultItem item : errItems) {

                String id = item.id;
                SinkRecord sinkRecord = recordMap.get(id);
                if (null == sinkRecord) {
                    continue;
                }
                if (item.status == 404 || item.status == 429 || item.status == 503 || item.status == 500 || item.status == 409) {
                    //索引不存在,拒绝，node异常，重新执行
                    insertRecordMap.put(id, sinkRecord);
                } else {
                    if (StringUtils.isBlank(sinkRecord.getRowKey())) {
                        continue;
                    }
                    if (null == item) {
                        continue;
                    }
                    String logInfo = "batchUpdateAction ===> Hbase rowkey=" + sinkRecord.getRowKey() + " Error item = [id : " + item.id + ", index : " + item.index + "type : " + item.type + ",operation :" + item.operation + ",status :" + item.status + ",version :" + item.version + ",error :" + item.error + ",errorType :" + item.errorType + ",errorReason :" + item.errorReason + "]";
                    logger.error(logInfo);
                }
            }

            if (!insertRecordMap.isEmpty()) {
//               插入数据分两种情况 我们在预处理时，recordMap已经对不同数据做过处理，故这里我们只需要根据不同的index 进行插入就可以
                result = batchInsertAction(insertRecordMap);
                if (!result.isSucceeded()) {
                    logger.warn("execute es error.msg=" + result.getErrorMessage());
                }
            }
        }
        return result;
    }

    /**
     * 会根据id 查询es 内的数据，如果 SinkRecord 的keyvalue 比es 新，需要先保存一份
     *
     * @param indexName
     * @param indexType
     * @param idList
     * @param pageSize
     * @return {@link Map< String, Map< String, Object>> }
     * @throw
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-07-02 19:02:09
     */
    public List<SinkRecord> doQuery_FromES(String indexName, String indexType, List<SinkRecord> idList, int pageSize) {
//        LOGGER.info("start --- doQuery_FromES thread = " + Thread.currentThread().getId());
        List<SinkRecord> result = new ArrayList<>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        for (SinkRecord record : idList) {
            Map<String, Object> keyValues = record.getKeyValues();
            String id = null;
            if (keyValues.containsKey(SEARCH_USER_ID)) {
                id = String.valueOf(keyValues.get(SEARCH_USER_ID));
            } else {
                id = String.valueOf(keyValues.get("id"));
            }
            boolQueryBuilder.should(QueryBuilders.termQuery("id", id));
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(boolQueryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(pageSize);

        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(indexName)
                .addType(indexType)
                .build();

        SearchResult sr = null;
        try {
            sr = client.execute(search);
        } catch (IOException e) {
            logger.warn("查询不存在的数据！" + sr.getErrorMessage());
        }
        if (!sr.isSucceeded()) {
            return result;
        }
        JsonObject jsonObject = sr.getJsonObject();
        long took = jsonObject.get("took").getAsLong();
        JsonObject hits = jsonObject.getAsJsonObject("hits");
        long total = hits.get("total").getAsLong();
        JsonArray docs = hits.getAsJsonArray("hits");
        Map<String, Map<String, Object>> resultMap = new HashMap<>();
        for (JsonElement element : docs) {
            if (!element.isJsonObject()) {
                continue;
            }
            Map map = JSON.parseObject(element.toString(), Map.class);
            if (MapUtils.isEmpty(map)) {
                continue;
            }
            if (!map.containsKey("_id") || StringUtils.isBlank(String.valueOf(map.get("_id")))) {
                continue;
            }
            String id = String.valueOf(map.get("_id"));
            if (!map.containsKey("_source") || MapUtils.isEmpty((Map<String, Object>) map.get("_source"))) {
                continue;
            }
            resultMap.put(id, (Map<String, Object>) map.get("_source"));
        }

        for (SinkRecord record : idList) {
            Map<String, Object> keyValues = record.getKeyValues();
            String id = "" + keyValues.get("id");
            Object userObj = keyValues.get(SEARCH_USER_ID);
            String userId = "" + userObj;
            if (userObj != null && StringUtils.isNotBlank(userId)) {
//                如果查询时候带了 USER_ID ，只更新user信息
                Map<String, Object> unionMap = resultMap.get(userId);
                keyValues.put(USER, unionMap);
                result.add(record);
                continue;
            }
            if (resultMap.containsKey(id)) {
                Map<String, Object> unionMap = resultMap.get(id);
                Object userIdObj = unionMap.get(USER_ID);
                String oldUserId = null;
                if (userIdObj != null) {
                    oldUserId = String.valueOf(userIdObj);
                }
                unionMap.putAll(keyValues);
                if (oldUserId!=null) {
                    unionMap.put("old"+USER_ID, oldUserId);
                }
                record.setKeyValues(unionMap);
                result.add(record);
            }
        }
        logger.info("查询总条数:" + total + ", took=" + took);
        logger.info("end --- doQuery_FromES thread = " + Thread.currentThread().getId());
        return result;
    }

}
