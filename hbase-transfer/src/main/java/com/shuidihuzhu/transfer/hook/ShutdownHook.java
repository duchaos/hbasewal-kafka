package com.shuidihuzhu.transfer.hook;

import com.shuidihuzhu.infra.gracefulshutdown.service.OrderedShutdown;
import com.shuidihuzhu.transfer.sink.ESSink;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.Ordered;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

/**
 * Created by sunfu on 2019/1/15.
 */
@Slf4j
public class ShutdownHook implements OrderedShutdown, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void pause() throws InterruptedException {

    }

    @Override
    public void shutdown(Integer delay) throws InterruptedException {
        if (applicationContext.containsBean(ESSink.class.getName())) {
            ESSink sink = applicationContext.getBean(ESSink.class);
            if (null != sink) {
                ExecutorService executorService = sink.getExecutorService();
                BlockingQueue queue = sink.getBlockingQueue();
                // 每200ms观测一次队列大小
                if (null != queue && queue.size() > 0) {
                    for (int i=0; i<5; i++) {
                        log.info("queue size=" + queue.size());
                        Thread.sleep(200);
                    }
                }
                if (!executorService.isShutdown()) {
                    executorService.shutdown();
                }
            }
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 102;
    }
}
