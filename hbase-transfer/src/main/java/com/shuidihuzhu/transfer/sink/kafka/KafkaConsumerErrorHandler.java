package com.shuidihuzhu.transfer.sink.kafka;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Service;

/**
 * @author duchao
 * @version : KafkaErrorHandler.java, v 0.1 2019-06-21 11:50 duchao Exp $
 */
@Slf4j
@Service
public class KafkaConsumerErrorHandler implements KafkaListenerErrorHandler {
    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
        log.error("KafkaConsumerErrorHandler.handleError message:{}", JSON.toJSONString(message),exception);
        return null;
    }

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {
        log.error("KafkaConsumerErrorHandler.handleError message:{}", JSON.toJSONString(message),exception);
        return null;
    }
}