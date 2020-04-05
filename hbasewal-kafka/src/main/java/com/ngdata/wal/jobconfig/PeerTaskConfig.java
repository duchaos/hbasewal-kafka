package com.ngdata.wal.jobconfig;

import lombok.Data;
import sun.misc.Contended;

/**
 * @author duchao
 * @version : PeerTaskConfig.java, v 0.1 2019-11-26 23:16 duchao Exp $
 */
@Data
public abstract class PeerTaskConfig {

    private String subscriptionName;

    @Contended("task")
    private volatile Boolean peerFlag = Boolean.TRUE;

    public abstract void init() throws Exception;

    public abstract void close();
}