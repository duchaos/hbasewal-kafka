package com.shuidihuzhu.transfer.sink.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.sink.AbstractSink;
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

    protected Logger logger = LoggerFactory.getLogger(ESSink.class);

    protected JestClient client;

    @Value("${hbase-transfer.elasticsearch.index}")
    protected String indexName;
    @Value("${hbase-transfer.elasticsearch.type}")
    protected String indexType;
    @Value("${hbase-transfer.elasticsearch.url}")
    protected String esUrl;
    @Value("${hbase-transfer.elasticsearch.username}")
    protected String username;
    @Value("${hbase-transfer.elasticsearch.password}")
    protected String password;


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
     * 这里说明方法的用途
     *
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
        result = afterUpdateProcess(bulkBuilder,recordMap, result);
        return result;
    }

    /**
     * 这里说明方法的用途
     *
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
        result = afterUpdateProcess(bulkBuilder, recordMap, result);
        return result;
    }

    protected JestResult batchInsertAction(Map<String, SinkRecord> recordMap, Bulk.Builder bulkBuilder) throws Exception {
        System.out.println("batchInsertAction ===> start...  " + recordMap.size());
        Map<String, String> idAndRowKeyMap = recordPreInsert(recordMap, bulkBuilder);
        JestResult result = client.execute(bulkBuilder.build());
        result = afterInsertProcess(recordMap, idAndRowKeyMap, result);
        return result;
    }


    protected Map<String, String> recordPreInsert(Map<String, SinkRecord> recordMap, Bulk.Builder bulkBuilder) {
        Index index = null;
        Map<String, String> idAndRowKeyMap = new HashMap();
        for (SinkRecord record : recordMap.values()) {
            String id = String.valueOf(record.getKeyValues().get("id"));
            if (!StringUtils.isEmpty(id)) {
//                    System.out.println("Insert rowkey = " + record.getRowKey() + ", column num: " + (record.getKeyValues().size() -1));
                idAndRowKeyMap.put(id, record.getRowKey());

                index = new Index.Builder(record.getKeyValues()).id(id).build();
            } else {
                index = new Index.Builder(record.getKeyValues()).build();
            }
            bulkBuilder.addAction(index).build();
        }
        return idAndRowKeyMap;
    }

    protected Map<String, SinkRecord> recordPreUpdate(List<SinkRecord> recordList, Bulk.Builder bulkBuilder) throws Exception {
        Update update = null;
        Map<String, SinkRecord> recordMap = new HashMap();
        for (SinkRecord record : recordList) {
            Map<String, Object> updateMap = record.getKeyValues();
//           这里的id ，可以是 用户画像的id ，也可以是 设备画像的id
//              id 的作用，就是 更新索引时候，锁定更新的document
            Object idObj = updateMap.get("id");
            String id = null;
            if (idObj != null) {
                id = String.valueOf(idObj);
            } else {
                throw new Exception("rowkey is null");
            }

            if (!StringUtils.isEmpty(id)) {
                Map<String, Map<String, Object>> docMap = new HashMap<>(1);
                docMap.put("doc", updateHandleWithBuilder(updateMap));
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

    /**
     * 这里需要区分到底是那种更新操作
     * ——————————————————————————————————————————————————————————————————————————————————————————————————————————————————
     * ||   用户画像的更新，放在 更新设备画像时操作，故不做处理
     * ||   设备画像的更新，需要区分是否包含 userId，
     * ||                a、不包含不做处理
     * ||                b、包含，需要进行如下操作
     * ||                   1、根据userId 获取对应用户画像全部数据，无视设备画像表传来的数据，放入更新内容中
     * ||                   2、根据设备id  对设备画像进行更新操作，
     * ||                       2.1 更新成功
     * ||                       2.2 更新失败
     * ||                              根据设备id ，获取对应设备画像全部数据，内存中进行目标字段更新替换，放入用户画像部分信息，进行index
     * ||
     * ——————————————————————————————————————————————————————————————————————————————————————————————————————————————————
     *
     * @param updateMap
     * @return {@link Map< String, Object> }
     * @throw
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-06-21 17:29:08
     */
    protected Map<String, Object> updateHandleWithBuilder(Map<String, Object> updateMap) {
        return updateMap;
    }

    protected JestResult afterInsertProcess(Map<String, SinkRecord> recordMap, Map<String, String> idAndRowKeyMap, JestResult result) throws Exception {
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
                Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
                result = batchInsertAction(rejectedRecordMap, bulkBuilder);
            }
        }
        return result;
    }

    protected JestResult afterUpdateProcess(Bulk.Builder bulkBuilder, Map<String, SinkRecord> recordMap, JestResult result) throws Exception {
        if (!result.isSucceeded()) {
            List<BulkResult.BulkResultItem> errItems = ((BulkResult) result).getFailedItems();
            Map<String, SinkRecord> insertRecordMap = new HashMap();
            for (BulkResult.BulkResultItem item : errItems) {

                String id = item.id;
                if (item.status == 404 || item.status == 429 || item.status == 503 || item.status == 500 || item.status == 409) {
                    //索引不存在,拒绝，node异常，重新执行
                    insertRecordMap.put(id, recordMap.get(id));
                } else {
                    String logInfo = "batchUpdateAction ===> Hbase rowkey=" + recordMap.get(id).getRowKey() + " Error item = [id : " + item.id + ", index : " + item.index + "type : " + item.type + ",operation :" + item.operation + ",status :" + item.status + ",version :" + item.version + ",error :" + item.error + ",errorType :" + item.errorType + ",errorReason :" + item.errorReason + "]";
                    logger.error(logInfo);
                }
            }

            if (!insertRecordMap.isEmpty()) {
//               插入数据分两种情况 我们在预处理时，recordMap已经对不同数据做过处理，故，这里我们只需要根据不同的index 进行插入就可以
                result = batchInsertAction(insertRecordMap, bulkBuilder);
                if (!result.isSucceeded()) {
                    throw new Exception("execute es error.msg=" + result.getErrorMessage());
                }
            }
        }
        return result;
    }

    protected JestResult searchDocumentById(String indexName, String typeName, String id) throws Exception {
        Get get = new Get.Builder(indexName, id).type(typeName).build();
        JestResult result = client.execute(get);
        if (!result.isSucceeded()) {
            throw new Exception("searchDocumentById error.msg=" + result.getErrorMessage());
        }
        return result;
    }

}
