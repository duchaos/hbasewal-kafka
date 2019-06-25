package com.shuidihuzhu.transfer.service;

import com.shuidihuzhu.sep.SepEvent;
import com.shuidihuzhu.transfer.enums.TransferEnum;
import com.shuidihuzhu.transfer.model.SinkRecord;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author duchao
 * @version : SepEventTranslator.java, v 0.1 2019-06-20 21:55 duchao Exp $
 */
public class SepEventTranslator {

    private SepEvent sepEvent;

    public void setBasicValues(SepEvent sepEvent) {
        this.sepEvent = sepEvent;
    }

    /**
     * Translate a data representation into fields set in given event
     */
    public TransferEnum translateTo(SinkRecord record) {
        TransferEnum transferEnum = TransferEnum.getEnumWithTable(Bytes.toString(sepEvent.getTable()));
        if (null==transferEnum){
            return null;
        }
        transferEnum.parse(record,sepEvent);
        clear(); // clear the translator
        return transferEnum;
    }

    /**
     * Release references held by this object to allow objects to be garbage-collected.
     */
    private void clear() {
        setBasicValues(null);
    }
}