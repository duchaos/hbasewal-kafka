/**
 * zhuanche.com Inc.
 * Copyright (c) 2015-2020 All Rights Reserved.
 */
package com.shuidihuzhu.transfer;

import java.io.File;
import java.util.concurrent.locks.StampedLock;

/**
 * @author duchao
 * @version : Test.java, v 0.1 2020/4/5 9:58 下午 duchao Exp $
 */
public class Test {
    public static void main(String[] args) {
        File topicDir = new File("/Users/duchao/Downloads/hbasewal-kafka/hbasewal-kafka/data/retry/");

        topicDir.mkdirs();
    }
}

