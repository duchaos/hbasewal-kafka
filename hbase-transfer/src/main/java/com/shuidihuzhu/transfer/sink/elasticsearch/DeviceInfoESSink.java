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


    @Override
    public void batchSink(List<SinkRecord> recordList) {
        try {
            Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(SDHZ_DEVICE_INFO_REALTIME.getIndex()).defaultType(SDHZ_DEVICE_INFO_REALTIME.getType());
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

            HashMap<String, Object> deviceUpdateMap = new HashMap<>(2);
            deviceUpdateMap.put(USER, map);
            map.put("id", userIdObj == null ? "" : String.valueOf(userIdObj));
            deviceUpdateMap.put("id", deviceId);
            return deviceUpdateMap;
        } else {
//            否则都是设备画像自己更新的,判断是否需要更userId
//            这里需要查询一次设备画像，获取现在的 dev_user_id
            Object idObj = map.get("id");
            if (null == idObj) {
                return map;
            }
            Object userIdInDeviceObj = map.get(USER_ID);
//            没带useId，直接返回
            if (null == userIdInDeviceObj) {
                logger.warn("DeviceInfoESSink.updateHandleWithBuilder 丢失用户id,deviceId:{}",deviceId);
                return map;
            }
            String userIdFromDevice = String.valueOf(userIdInDeviceObj);
            String device_id = String.valueOf(idObj);
            try {
                JestResult jestResult = searchDocumentById(SDHZ_DEVICE_INFO_REALTIME.getIndex(), SDHZ_DEVICE_INFO_REALTIME.getType(), device_id);
                if (!jestResult.isSucceeded()) {
                    return map;
                }
//                resultMap 是查询到原有的设备信息map
                Map<String, Object> resultMap = jestResult.getSourceAsObject(Map.class);
                if (MapUtils.isEmpty(resultMap)) {
                    return map;
                }
//              获取userId ,和map中的对比，如果有变更，拉去map信息，没有则直接返回
                Object userIdObj = resultMap.get(USER_ID);
                if (null == userIdObj) {
                    return map;
                }
                String userId = String.valueOf(userIdObj);
//           userId 没有改变，不需要更新用户信息，直接返回
                if (userIdFromDevice.equals(userId)) {
                    return map;
                }

//        userId 改变，需要拉去最新的userInfo
                JestResult userResult = searchDocumentById(SDHZ_USER_INFO_REALTIME.getIndex(), SDHZ_USER_INFO_REALTIME.getType(), userIdFromDevice);
//            用户信息获取失败，不做处理
                if (!userResult.isSucceeded()) {
                    return map;
                }
                Map<String, Object> newUserInfoMap = jestResult.getSourceAsObject(Map.class);
                if (MapUtils.isEmpty(newUserInfoMap)) {
                    return map;
                }
                Map<String, Object> userInfoMap = new HashMap<>();
                for (String key : newUserInfoMap.keySet()) {
                    if (!key.contains("es_metadata")) {
                        userInfoMap.put(key, newUserInfoMap.get(key));
                    }
                }
//           放入新的用户信息
                map.put(USER, userInfoMap);
            } catch (Exception e) {
                logger.error("DeviceInfoESSink.updateHandleWithBuilder userId:{}", userIdFromDevice, e);
            }
        }
        return map;
    }

    @Override
    public JestResult afterUpdateProcess(Map<String, SinkRecord> recordMap, JestResult result) throws Exception {
        if (MapUtils.isEmpty(recordMap)) {
            return result;
        }
//        这里来判断是否有更新user信息，更新的需要 insert 到 设备画像
        Map<String, SinkRecord> deviceUpdateMap = new HashMap<>();
        for (Map.Entry<String, SinkRecord> entry : deviceUpdateMap.entrySet()) {
            SinkRecord record = entry.getValue();
            if (null == record) {
                continue;
            }
            Map<String, Object> keyValues = record.getKeyValues();
            if (MapUtils.isEmpty(keyValues)) {
                continue;
            }
            Map<String, Object> userInfoMap = (Map<String, Object>) keyValues.get(USER);
            if (MapUtils.isEmpty(userInfoMap)) {
                continue;
            }
            deviceUpdateMap.put(entry.getKey(), entry.getValue());
        }
        if (MapUtils.isNotEmpty(deviceUpdateMap)) {
            batchInsertAction(deviceUpdateMap);
        }
        return super.afterUpdateProcess(recordMap, result);
    }

    @Override
    public JestResult batchInsertAction(Map<String, SinkRecord> rejectedRecordMap) throws Exception {
        Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(SDHZ_DEVICE_INFO_REALTIME.getIndex()).defaultType(SDHZ_DEVICE_INFO_REALTIME.getType());
        return batchInsertAction(rejectedRecordMap, bulkBuilder);
    }

}