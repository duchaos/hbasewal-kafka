package com.shuidihuzhu.transfer.controller;

import com.google.common.collect.Maps;
import com.shuidihuzhu.transfer.model.Response;
import com.shuidihuzhu.transfer.model.SinkRecord;
import com.shuidihuzhu.transfer.sink.elasticsearch.ESSink;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;
import java.util.UUID;

@Controller
@RequestMapping("/api/hbase-transfer/es")
public class EsController {
    private Logger logger = LoggerFactory.getLogger(EsController.class);

    @Autowired
    private ESSink esSink;

    @RequestMapping(value="/add", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Response add(String rowkey){
        try{
            SinkRecord record = new SinkRecord();
            if(StringUtils.isEmpty(rowkey)){
                rowkey=UUID.randomUUID().toString();
            }
            record.setRowKey(rowkey);

            System.out.println(rowkey);
            Map<String, Object> keyValues = Maps.newHashMap();
            keyValues.put("xx","yy");
            record.setKeyValues(keyValues);
            esSink.sink(record);
        }catch (Exception e){
            logger.error("add peer error.",e);
            return Response.getInstance(Response.SYSTEM_SERVER_ERROR);
        }
        return Response.makeSuccess(null);
    }

}