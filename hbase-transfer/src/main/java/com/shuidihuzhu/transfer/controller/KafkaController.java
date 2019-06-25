package com.shuidihuzhu.transfer.controller;

import com.shuidihuzhu.transfer.model.Response;
import com.shuidihuzhu.transfer.sink.elasticsearch.ESSink;
import com.shuidihuzhu.transfer.sink.kafka.HKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/api/hbase-transfer/kafka")
public class KafkaController {
    private Logger logger = LoggerFactory.getLogger(KafkaController.class);

    @Autowired
    private HKafkaConsumer hKafkaConsumer;

    @Autowired
    private ESSink userInfoESSink;

    @RequestMapping(value="/consumer", method = {RequestMethod.GET, RequestMethod.POST})
    public @ResponseBody Response add(String groupId,String topic,String fromStart){
        try{
            hKafkaConsumer.consumer(groupId, topic, fromStart,userInfoESSink);
        }catch (Exception e){
            logger.error("add peer error.",e);
            return Response.getInstance(Response.SYSTEM_SERVER_ERROR);
        }
        return Response.makeSuccess(null);
    }

}