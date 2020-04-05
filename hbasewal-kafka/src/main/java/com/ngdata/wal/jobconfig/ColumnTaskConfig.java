package com.ngdata.wal.jobconfig;

import lombok.Data;
import sun.misc.Contended;

/**
 * @author duchao
 * @version : ColumnTaskConfig.java, v 0.1 2019-11-26 23:27 duchao Exp $
 */
@Data
public class ColumnTaskConfig extends FamilyTaskConfig {
    private String columnName;

    @Contended("task")
    private volatile Boolean columnFlag = Boolean.TRUE;

    @Override
    public void init() throws Exception {

    }

    @Override
    public void close() {

    }
}