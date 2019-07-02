package com.shuidihuzhu.transfer.enums;

import com.google.common.collect.Maps;
import com.shuidihuzhu.sep.SepEvent;
import com.shuidihuzhu.transfer.model.SinkRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * @author duchao
 * @version : TransferEnum.java, v 0.1 2019-06-20 21:14 duchao Exp $
 */
@Slf4j
public enum TransferEnum {

    SDHZ_DEVICE_INFO_REALTIME {
        public String getTable() {
            return "sdhz_device_info_realtime";
        }

        public String getIndex() {
            return "sdhz_device_info_realtime";
        }

        public String getType() {
            return "detail";
        }

        public String getTopic() {
            return "hbase-tranfer-device";
        }

        public TransferEnum  syncToIndexEnum() {
            return null;
        }

        public SinkRecord parse(SinkRecord record, SepEvent sepEvent) {
            String payload = Bytes.toString(sepEvent.getPayload());
            Map<String, Object> keyValues = Maps.newHashMap();
            String rowKey = null;
            String id = null;
            boolean isErrRowKey = false;
            for (Cell cell : sepEvent.getKeyValues()) {
                rowKey = Bytes.toString(CellUtil.cloneRow(cell));

                if (!rowKey.contains(":") || rowKey.split(":").length < 2) {
                    log.warn("Discarded --- rowKey=" + rowKey);
                    isErrRowKey = true;
                    break;
                }

                id = rowKey.split(":")[1];
                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                record.setTable(this.getTable());
                record.setFamily(family);
                record.setQualifier(qualifier);
                record.setRowKey(rowKey);
                record.setTimestamp(timestamp);
                record.setValue(value);
                record.setPayload(payload);

                String column = "";
                if (family.equals("data")) {
                    column = family + "_" + qualifier;
                } else {
                    column = qualifier;
                }

                if (column.contains(".")) {
                    column = column.replace(".", "_");
                }

                record.setColumn(column);
                keyValues.put(column, value);
            }

            if (isErrRowKey) {
                return null;
            }

            keyValues.put("id", id);
            record.setKeyValues(keyValues);
            return record;
        }
    },
    SDHZ_USER_INFO_REALTIME {
        public String getTable() {
            return "sdhz_user_info_realtime";
        }

//        FIXME 需要和 config 里保持一致
        public String getTopic() {
            return "hbase-tranfer-20190527";
        }

        public String getIndex() {
            return "sdhz_user_info_realtime";
        }

        public String getType() {
            return "detail";
        }

        public TransferEnum syncToIndexEnum() {
            return SDHZ_DEVICE_INFO_REALTIME;
        }

        public SinkRecord parse(SinkRecord record, SepEvent sepEvent) {
            String payload = Bytes.toString(sepEvent.getPayload());
            Map<String, Object> keyValues = Maps.newHashMap();
            String rowKey = null;
            String id = null;
            boolean isErrRowKey = false;
            for (Cell cell : sepEvent.getKeyValues()) {
                rowKey = Bytes.toString(CellUtil.cloneRow(cell));

                if (rowKey.contains("-") || !rowKey.contains(":") || rowKey.split(":").length < 2) {
                    log.warn("Discarded --- rowKey=" + rowKey);
                    isErrRowKey = true;
                    break;
                }

                id = rowKey.split(":")[1];

                try {
                    Long.parseLong(id);
                } catch (NumberFormatException e) {
                    log.warn("Discarded --- rowKey=" + rowKey + ", not long type : " + id);
                    isErrRowKey = true;
                    break;
                }

                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                record.setTable(this.getTable());
                record.setFamily(family);
                record.setQualifier(qualifier);
                record.setRowKey(rowKey);
                record.setTimestamp(timestamp);
                record.setValue(value);
                record.setPayload(payload);

                String column = "";
                if (family.equals("data")) {
                    column = family + "_" + qualifier;
                } else {
                    column = qualifier;
                }

                if (column.contains(".")) {
                    column = column.replace(".", "_");
                }

                record.setColumn(column);
                keyValues.put(column, value);
            }

            if (isErrRowKey) {
                return null;
            }

            keyValues.put("id", id);
            record.setKeyValues(keyValues);
            return record;
        }
    };

    TransferEnum() {
    }

    public SinkRecord parse(SinkRecord record, SepEvent sepEvent) {
        throw new AbstractMethodError();
    }

    public String getTable() {
        throw new AbstractMethodError();
    }

    public String getTopic() {
        throw new AbstractMethodError();
    }

    public String getIndex() {
        throw new AbstractMethodError();
    }

    public String getType() {
        throw new AbstractMethodError();
    }

    public TransferEnum syncToIndexEnum() {
        throw new AbstractMethodError();
    }

    public static TransferEnum getEnumWithTable(String topic) {
        for (TransferEnum value : values()) {
            if (value.getTable().equals(topic)) {
                return value;
            }
        }
        return null;
    }

}
