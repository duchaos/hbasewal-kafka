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
import io.searchbox.core.Update;
import io.searchbox.core.UpdateByQuery;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.lucene.document.Document;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequestBuilder;
import org.elasticsearch.index.search.SimpleQueryStringQueryParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

/**
 * Created by sunfu on 2018/12/29.
 */
@Service
public class ESSink extends AbstractSink implements InitializingBean {

    private JestClient client;
    @Override
    public void afterPropertiesSet() throws Exception {
        JestClientFactory factory = new JestClientFactory();

        String ELASTIC_SEARCH_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
        Gson gson = new GsonBuilder()
                .setDateFormat(ELASTIC_SEARCH_DATE_FORMAT)
                .create();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://10.100.2.3:9201")
                .multiThreaded(true)
                .defaultMaxTotalConnectionPerRoute(2)
                .connTimeout(3600000)
                .readTimeout(3600000)
                .gson(gson)
                .maxTotalConnection(10).build());

        client = factory.getObject();

    }

    @Override
    public void sink(SinkRecord record) {
        SearchSourceBuilder sourceBuilder = null;
        JestResult result = null;
        try{
//            String fieldName = "field";
//            sourceBuilder = buildSearch(record,fieldName);
//            String qslStr = sourceBuilder.toString();
//            JSONObject scriptObject = JSON.parseObject(qslStr);
//            JSONObject updateByQueryJson = new JSONObject();
//            scriptObject.getJSONObject("query");
//
//            updateByQueryJson.put("query",scriptObject.getJSONObject("query"));
//            updateByQueryJson.put("script",scriptObject.getJSONObject("script_fields").getJSONObject(fieldName).getJSONObject("script"));
            UpdateByQuery updateByQuery = new UpdateByQuery.Builder(buildSearch(record))
                    .addIndex("sdhz_user_info_realtime")
                    .addType("detail")
                    .build();

            result = client.execute(updateByQuery);
        }catch (Exception e){
            handleErrorRecord(record);
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
        scriptMap.put("source","ctx._source."+record.getFamily()+"_"+record.getQualifier()+"='"+record.getValue()+"'");
        scriptMap.put("lang","painless");
        scriptObject.put("script", scriptMap);
        return scriptObject.toString();
    }

    @Override
    public void batchSink(List<SinkRecord> records) {

    }


}
