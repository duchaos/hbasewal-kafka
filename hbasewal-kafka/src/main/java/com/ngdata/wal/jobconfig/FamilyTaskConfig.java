package com.ngdata.wal.jobconfig;

import lombok.Data;
import sun.misc.Contended;

/**
 * @author duchao
 * @version : FamilyTaskConfig.java, v 0.1 2019-11-26 23:26 duchao Exp $
 */
@Data
public class FamilyTaskConfig extends TableTaskConfig {

    private String familyName;

    @Contended("task")
    private volatile Boolean familyFlag = Boolean.TRUE;

    @Override
    public void init() throws Exception {

    }

    @Override
    public void close() {

    }
}