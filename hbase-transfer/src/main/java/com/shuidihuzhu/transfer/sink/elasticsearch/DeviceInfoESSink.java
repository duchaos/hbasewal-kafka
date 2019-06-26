package com.shuidihuzhu.transfer.sink.elasticsearch;

import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.shuidihuzhu.transfer.enums.TransferEnum.SDHZ_DEVICE_INFO_REALTIME;
import static com.shuidihuzhu.transfer.enums.TransferEnum.SDHZ_USER_INFO_REALTIME;

/**
 * @author duchao
 * @version : UserInfoESSink.java, v 0.1 2019-06-21 15:03 duchao Exp $
 */
@Service
public class DeviceInfoESSink extends ESSink {

    private static final String DEVICE_ID = "data_device_id";
    private static final String USER = "user";
    private static final String USER_ID = "data_dev_user_id";
    private Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(SDHZ_DEVICE_INFO_REALTIME.getIntex()).defaultType(SDHZ_DEVICE_INFO_REALTIME.getType());



    @Override
    public void batchSink(List<SinkRecord> recordList) {
        try {
            batchUpdateAction(recordList, bulkBuilder);
        } catch (Exception e) {
            logger.error("DeviceInfoESSink.batchSink into es error.", e);
            handleBatchErrorRecord(recordList);
        }
    }

    /**
     * 需要区分是否包含 userId，
     * a、不包含不做处理
     * b、包含，需要进行如下操作
     * 1、根据userId 获取对应用户画像全部数据，无视设备画像表传来的数据，放入更新内容中
     *
     * @param map
     * @return {@link Map< String, Object> }
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-06-21 17:45:16
     */
    @Override
    public Map<String, Object> updateHandleWithBuilder(Map<String, Object> map) {
//       如果是用户画像调用的，必含DEVICE_ID
        Object deviceIdObj = map.get(DEVICE_ID);
        String deviceId = String.valueOf(deviceIdObj);
        if (null != deviceIdObj && StringUtils.isNotBlank(deviceId)) {
//           deviceId有效，说明用户画像更新调用,需要替换后续index 的id
            Object userIdObj = map.get("id");
            map.put("id", deviceId);
//            用户画像带来的信息，都是属于user 的
            map.remove(DEVICE_ID);

            HashMap<String, Object> hashMap = new HashMap<>(2);
            hashMap.put(USER, map);
            map.put("id", userIdObj == null ? "" : String.valueOf(userIdObj));
            hashMap.put("id", deviceId);
            return hashMap;
        } else {
//            否则都是设备画像自己更新的
            Object userIdObj = map.get(USER_ID);
            if (null == userIdObj) {
                return map;
            }

            String userId = String.valueOf(userIdObj);
            if (StringUtils.isBlank(userId)) {
                return map;
            }
//        包含userId，根据userId 查询es
            try {
                JestResult jestResult = searchDocumentById(SDHZ_USER_INFO_REALTIME.getIntex(), SDHZ_USER_INFO_REALTIME.getType(), userId);
                if (jestResult.isSucceeded()) {
                    Map<String, Object> resultMap = jestResult.getSourceAsObject(Map.class);
                    if (MapUtils.isEmpty(resultMap)) {
                        return map;
                    }
                    Map<String, Object> userInfoMap = new HashMap<>();
                    for (String key : resultMap.keySet()) {
                        if (!key.contains("es_metadata")) {
                            userInfoMap.put(key, resultMap.get(key));
                        }
                    }
                    map.put(USER, userInfoMap);
                }

            } catch (Exception e) {
                logger.error("DeviceInfoESSink.updateHandleWithBuilder userId:{}", userId, e);
            }
        }
        return map;
    }

    @Override
    public JestResult batchInsertAction(Map<String, SinkRecord> rejectedRecordMap) throws Exception {
        return batchInsertAction(rejectedRecordMap, bulkBuilder);
    }

    @Override
    public JestResult afterUpdateProcess(Map<String, SinkRecord> recordMap, JestResult result) throws Exception {
        if (MapUtils.isEmpty(recordMap)) {
            return result;
        }
//       查设备信息
        for (Map.Entry<String, SinkRecord> entry : recordMap.entrySet()) {
            SinkRecord record = entry.getValue();
            if (record == null) {
                continue;
            }
            Map<String, Object> keyValues = record.getKeyValues();
            if (MapUtils.isEmpty(keyValues)) {
                continue;
            }
            String deviceId = entry.getKey();
            JestResult jestResult = searchDocumentById(SDHZ_DEVICE_INFO_REALTIME.getIntex(), SDHZ_DEVICE_INFO_REALTIME.getType(), deviceId);
            if (jestResult.isSucceeded()) {
//                resultMap 是查询到原有的设备信息map
                Map<String, Object> resultMap = jestResult.getSourceAsObject(Map.class);
                if (MapUtils.isEmpty(resultMap)) {
                    continue;
                }
//               原有的map.addAll(新的map，除用户信息)
                Map<String, Object> deviceInfoMap = new HashMap<>();
                for (String key : resultMap.keySet()) {
                    if (!key.contains("user")) {
                        deviceInfoMap.put(key, resultMap.get(key));
                    }
                }
                deviceInfoMap.putAll(keyValues);
                record.setKeyValues(deviceInfoMap);
            }
        }
        return batchInsertAction(recordMap, bulkBuilder);
    }

}