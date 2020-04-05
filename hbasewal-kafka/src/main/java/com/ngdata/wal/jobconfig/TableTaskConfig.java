package com.ngdata.wal.jobconfig;

import lombok.Data;
import sun.misc.Contended;

/**
 * @author duchao
 * @version : TableTaskConfig.java, v 0.1 2019-11-26 23:23 duchao Exp $
 */
@Data
public class TableTaskConfig extends PeerTaskConfig {

    private String tableName;
    @Contended("task")
    private volatile Boolean tableFlag = Boolean.TRUE;

    @Override
    public void init() throws Exception {

    }

    @Override
    public void close() {

    }
}