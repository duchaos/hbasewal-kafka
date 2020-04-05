package com.shuidihuzhu.transfer.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.core.UpdateByQuery;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * Created by sunfu on 2018/12/29.
 */
public class Test {

    private Logger logger = LoggerFactory.getLogger(Test.class);

    private static JestClient client;

    @Value("${hbase-transfer.elasticsearch.index}")
    private String indexName;
    @Value("${hbase-transfer.elasticsearch.type}")
    private String indexType;

    public static void main() throws Exception {
        JestClientFactory factory = new JestClientFactory();

        String ELASTIC_SEARCH_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        Gson gson = new GsonBuilder()
                .setDateFormat(ELASTIC_SEARCH_DATE_FORMAT)
                .create();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://10")
                .multiThreaded(true)
                .defaultMaxTotalConnectionPerRoute(2)
                .connTimeout(3600000)
                .readTimeout(3600000)
                .gson(gson)
                .maxTotalConnection(10).build());

        client = factory.getObject();

    }

    public void sink(SinkRecord record) {
        try{
            JestResult updateResult = updateAction(record);

            if(updateResult.isSucceeded() && updateResult.getJsonObject().get("updated").getAsBigInteger().intValue()==0){
                insertAction(record);
            }
        }catch (Exception e){
//            handleErrorRecord(record);
        }
    }

    public JestResult updateAction(SinkRecord record) throws Exception{
        record.getKeyValues().put("id",record.getRowKey());
        UpdateByQuery updateByQuery = new UpdateByQuery.Builder(buildSearch(record))
                .addIndex(indexName)
                .addType(indexType)
                .build();

        return client.execute(updateByQuery);
    }

    public void insertAction(SinkRecord record) throws Exception{
        Bulk.Builder bulkBuilder =new Bulk.Builder().defaultIndex(indexName).defaultType(indexType);
        Index index = null;
        if(!StringUtils.isEmpty(record.getRowKey())){
            index = new Index.Builder(record.getKeyValues()).id(record.getRowKey()).build();
        }else{
            index = new Index.Builder(record.getKeyValues()).build();
        }
        bulkBuilder.addAction(index).build();

        JestResult result = client.execute(bulkBuilder.build());
        if(!result.isSucceeded()){
            throw new Exception("execute es error.msg="+result.getErrorMessage());
        }
    }

    private String buildSearch(SinkRecord record) {
        //指定查询的库表
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        if (record != null) {
            //构建查询条件必须嵌入filter中！
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if(!StringUtils.isEmpty(record.getRowKey())){
                boolQueryBuilder.must(QueryBuilders.termQuery("id.keyword",record.getRowKey()));
            }else{
                throw new RuntimeException("binlog dont contain id");
            }

            searchSourceBuilder.query(boolQueryBuilder);
        }
        JSONObject scriptObject = JSON.parseObject(searchSourceBuilder.toString());
        Map<String,String> scriptMap = Maps.newHashMap();
        String script = "";
        for(Map.Entry<String,Object> tmp : record.getKeyValues().entrySet()){
            script = script+"ctx._source."+tmp.getKey()+"='"+tmp.getValue()+"';";
        }
        scriptMap.put("source",script);
        scriptMap.put("lang","painless");
        scriptObject.put("script", scriptMap);
        return scriptObject.toString();
    }

}
