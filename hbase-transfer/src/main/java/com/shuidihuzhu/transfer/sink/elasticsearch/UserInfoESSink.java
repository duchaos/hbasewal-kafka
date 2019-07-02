package com.shuidihuzhu.transfer.sink.elasticsearch;

import com.shuidihuzhu.transfer.enums.TransferEnum;
import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.shuidihuzhu.transfer.enums.TransferEnum.SDHZ_USER_INFO_REALTIME;

/**
 * @author duchao
 * @version : UserInfoESSink.java, v 0.1 2019-06-21 15:03 duchao Exp $
 */
@Service
public class UserInfoESSink extends ESSink {
    private static final String DEVICE_ID = "data_device_id";

    @Resource
    private DeviceInfoESSink deviceInfoESSink;


    @Override
    public void batchSink(List<SinkRecord> recordList) {
        try {
            Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(SDHZ_USER_INFO_REALTIME.getIndex()).defaultType(SDHZ_USER_INFO_REALTIME.getType());
            JestResult jestResult = batchUpdateAction(recordList, bulkBuilder);
            if (!jestResult.isSucceeded()) {
                logger.error("UserInfoESSink.batchSink error！");
                return;
            }

        } catch (Exception e) {
            logger.error("UserInfoESSink.batchSink into es error.", e);
            handleBatchErrorRecord(recordList);
        }
        try {
//           获取更新结果，更新成功后，需要同步更新 设备画像表
            TransferEnum sycnToIndex = SDHZ_USER_INFO_REALTIME.syncToIndexEnum();
            if (null != sycnToIndex) {
                Bulk.Builder syncBuild = new Bulk.Builder().defaultIndex(sycnToIndex.getIndex()).defaultType(sycnToIndex.getType());
//                获取需要同步到设备画像的用户集合
                List<SinkRecord> sinkRecords = recordList.stream().filter(
                        record -> {
                            Map<String, Object> map = record.getKeyValues();
                            if (MapUtils.isEmpty(map)) {
                                return false;
                            }
                            Object idObj = map.get("id");
                            if (idObj == null || StringUtils.isBlank("" + idObj)) {
                                return false;
                            }
                            if (!map.containsKey(DEVICE_ID)) {
                                return false;
                            }
                            return true;
                        }).collect(Collectors.toList());

                if (CollectionUtils.isEmpty(sinkRecords)) {
                    return;
                }

//               获取用户id 和 用户信息的集合
                List<SinkRecord> sinkRecordList = doQuery_FromES(SDHZ_USER_INFO_REALTIME.getIndex(), SDHZ_USER_INFO_REALTIME.getType(), sinkRecords, sinkRecords.size());

                for (SinkRecord record : sinkRecordList) {
                    Map<String, Object> userInfoMap = record.getKeyValues();
                    if (MapUtils.isEmpty(userInfoMap)) {
                        return;
                    }
                    Map<String, Object> paramMap = new HashMap<>();
                    for (Map.Entry<String, Object> user : userInfoMap.entrySet()) {
                        if (!user.getKey().contains("es_metadata")) {
                            paramMap.put(user.getKey(), user.getValue());
                        }
                    }
                    record.setKeyValues(paramMap);
                }
                deviceInfoESSink.batchUpdateAction(sinkRecordList, syncBuild);
            }
        } catch (
                Exception e) {
            logger.error("UserInfoESSink.batchSink into es error.", e);
            handleBatchErrorRecord(recordList);
        }

    }

    @Override
    public JestResult batchInsertAction(Map<String, SinkRecord> rejectedRecordMap) throws Exception {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(SDHZ_USER_INFO_REALTIME.getIndex()).defaultType(SDHZ_USER_INFO_REALTIME.getType());
        return batchInsertAction(rejectedRecordMap, bulkBuilder);
    }
}
