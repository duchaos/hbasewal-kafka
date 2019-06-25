package com.shuidihuzhu.transfer.sink.elasticsearch;

import com.shuidihuzhu.transfer.enums.TransferEnum;
import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(SDHZ_USER_INFO_REALTIME.getIntex()).defaultType(SDHZ_USER_INFO_REALTIME.getType());

    @Override
    public void batchSink(List<SinkRecord> recordList) {
        try {
            JestResult jestResult = batchUpdateAction(recordList, bulkBuilder);
            if (!jestResult.isSucceeded()) {
                logger.error("UserInfoESSink.batchSink error！");
                return;
            }
//           获取更新结果，更新成功后，需要同步更新 设备画像表
            TransferEnum sycnToIndex = SDHZ_USER_INFO_REALTIME.syncToIndexEnum();
            if (null != sycnToIndex) {
                Bulk.Builder syncBuild = new Bulk.Builder().defaultIndex(sycnToIndex.getIntex()).defaultType(sycnToIndex.getType());
                SinkRecord sinkRecord = recordList.get(0);
                Map<String, Object> valueMap = sinkRecord.getKeyValues();
                if (MapUtils.isEmpty(valueMap)) {
                    return;
                }
                Object idObj = valueMap.get("id");
                if (idObj == null || StringUtils.isBlank("" + idObj)) {
                    return;
                }

                JestResult userInfoResult = searchDocumentById(SDHZ_USER_INFO_REALTIME.getIntex(), SDHZ_USER_INFO_REALTIME.getType(), String.valueOf(idObj));
                if (userInfoResult.isSucceeded()) {
                    Map<String, Object> userInfoMap = userInfoResult.getSourceAsObject(Map.class);
                    if (MapUtils.isEmpty(userInfoMap)) {
                        return;
                    }
                    if (userInfoMap.containsKey(DEVICE_ID)) {
                        Map<String, Object> paramMap = new HashMap<>();
                        for (Map.Entry<String, Object> entry : userInfoMap.entrySet()) {
                            if (!entry.getKey().contains("es_metadata")) {
                                paramMap.put(entry.getKey(), entry.getValue());
                            }
                        }
                        sinkRecord.setKeyValues(paramMap);
                        deviceInfoESSink.batchUpdateAction(recordList, syncBuild);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("UserInfoESSink.batchSink into es error.", e);
            handleBatchErrorRecord(recordList);
        }
    }

}