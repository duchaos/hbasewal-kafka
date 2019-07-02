package com.shuidihuzhu.transfer.sink.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import io.searchbox.core.Update;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.shuidihuzhu.transfer.enums.TransferEnum.SDHZ_DEVICE_INFO_REALTIME;
import static com.shuidihuzhu.transfer.enums.TransferEnum.SDHZ_USER_INFO_REALTIME;

/**
 * @author duchao
 * @version : UserInfoESSink.java, v 0.1 2019-06-21 15:03 duchao Exp $
 */
@Service
public class DeviceInfoESSink extends ESSink {

    private static final String DEVICE_ID = "data_device_id";
    public static final String USER = "user";
    public static final String USER_ID = "data_dev_user_id";
    public static final String SEARCH_USER_ID = "search_user_id";


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
     * @return {@link Map< String, Object> }
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-06-21 17:45:16
     */
    @Override
    public Map<String, SinkRecord> recordPreUpdate(List<SinkRecord> recordList, Bulk.Builder bulkBuilder) throws Exception {
        Map<String, SinkRecord> recordMap = new HashMap();
        if (CollectionUtils.isEmpty(recordList)) {
            logger.error("ESSink.recordPreUpdate recordList is empty!");
            return recordMap;
        }
        /* 放到最后的逻辑
         * 1、map 盘空
         * 2、id 有效
         * 3、组装doc map 数据，这里才需要区分 用户更新的 设备更新的
         * 4、把更新的json -》update -》bulkBuilder.addAction
         * 5、更新recordMap
         * */
//        1、过滤掉id不符合规范部分
        recordList = recordList.stream().filter(record -> {
            Map<String, Object> updateMap = record.getKeyValues();
//           1、map 盘空
            if (MapUtils.isEmpty(updateMap)) {
                return false;
            }
//         2、id 有效
            Object idObj = updateMap.get("id");
            String id = null;
            if (idObj == null) {
                logger.error("rowkey is null");
                return false;
            }
            id = String.valueOf(idObj);
            if (StringUtils.isBlank(id)) {
                return false;
            }
            return true;
        }).collect(Collectors.toList());

        /** 2、区分 用户画像调用 和 设备画像调用*/

//        过滤出用户画像调用的id
        List<SinkRecord> userUpdateList = new ArrayList<>();
//        过滤出设备画像调用
        List<SinkRecord> deviceUpdateList = new ArrayList<>();
        for (SinkRecord sinkRecord : recordList) {

            Map<String, Object> keyValues = sinkRecord.getKeyValues();
//            用户画像更新后，通过DEVICE_ID 同步设备画像
            if (keyValues.containsKey(DEVICE_ID) && StringUtils.isNotBlank("" + keyValues.get(DEVICE_ID))) {
                userUpdateList.add(sinkRecord);
            } else {
                deviceUpdateList.add(sinkRecord);
            }
        }

//        用户画像更新操作后的，recordMap 这里需要做的就是，把用户信息map ，封装成 带user 的map ,后续会识别这个
        if (!CollectionUtils.isEmpty(userUpdateList)) {
            userUpdateList.forEach(record -> {
                HashMap<String, Object> keyValueMap = new HashMap<>(2);
                Map<String, Object> userInfoMap = record.getKeyValues();
                String id = "" + userInfoMap.get("id");
                String deviceId = "" + userInfoMap.get(DEVICE_ID);
                keyValueMap.put(USER, userInfoMap);
                keyValueMap.put("id", deviceId);
                record.setKeyValues(keyValueMap);
                Map<String, Map<String, Object>> docMap = new HashMap<>(1);
                docMap.put("doc", keyValueMap);
                Update update = new Update.Builder(JSON.toJSONString(docMap)).id(deviceId).build();
                bulkBuilder.addAction(update).build();
                if (recordMap.containsKey(id)) {
                    recordMap.get(id).getKeyValues().putAll(keyValueMap);
                } else {
                    recordMap.put(id, record);
                }
            });
        }

//        设备画像更新操作后的，recordMap
        if (!CollectionUtils.isEmpty(deviceUpdateList)) {
//          直接更新
            List<SinkRecord> normalDeviceInfoList = new ArrayList<>();
//          需要检查user信息的
            List<SinkRecord> needCheckUserInfoList = new ArrayList<>();
//          新增设备画像的
            List<SinkRecord> createDeviceInfoList = new ArrayList<>();

//  设备信息list,主要是为了获取用户id
            List<SinkRecord> searchDeviceInfoList = new ArrayList<>(deviceUpdateList);
//          获取es 中 设备画像的信息
            List<SinkRecord> searchResultList = doQuery_FromES(SDHZ_DEVICE_INFO_REALTIME.getIndex(), SDHZ_DEVICE_INFO_REALTIME.getType(), searchDeviceInfoList, searchDeviceInfoList.size());
//          查询的结果 设备id 和 用户id 映射,
//          没有用户id，或者 新增设备画像 不存在
            Map<String, String> searchResultMap = new HashMap<>();
            Map<String, SinkRecord> noUserIdMap = new HashMap<>();
            for (SinkRecord record : searchResultList) {
                Map<String, Object> keyValues = record.getKeyValues();
                if (MapUtils.isEmpty(keyValues)) {
                    continue;
                }
                String device_id = "" + keyValues.get("id");
                Object userIdObj = keyValues.get(USER_ID);
                if (null == userIdObj) {
                    logger.warn("DeviceInfoESSink.updateHandleWithBuilder  deviceId:{},userid is null!", device_id);
                    noUserIdMap.put("" + keyValues.get("id"), record);
                    continue;
                }
                if (keyValues.containsKey("old"+USER_ID)){
                    String userId = String.valueOf(keyValues.get("old" + USER_ID));
                    searchResultMap.put("" + keyValues.get("id"), String.valueOf(userId));
                }else {
                    searchResultMap.put("" + keyValues.get("id"), String.valueOf(userIdObj));
                }
            }

            deviceUpdateList.forEach(record -> {

                //            否则都是设备画像自己更新的,判断是否需要更userId
//            这里需要查询一次设备画像，获取现在的 dev_user_id
                Map<String, Object> keyValues = record.getKeyValues();
                String deviceId = "" + keyValues.get("id");
                Object userIdInDeviceObj = keyValues.get(USER_ID);
//            没带useId,不需往下查用户了,直接更新就好
//             有用户id ，需要看是否有变化，有变化才继续
//                第一步，查userid
//                第二部，看是否变化
                if (null != userIdInDeviceObj) {
                    String userIdFromDevice = String.valueOf(userIdInDeviceObj);
                    String userIdFromEs = null;
                    if (searchResultMap.get(deviceId) != null) {
                        userIdFromEs = searchResultMap.get(deviceId);
//                        之前没有用户id，现在添加用户id的
                    } else if (noUserIdMap.get(deviceId) != null) {
                        needCheckUserInfoList.add(record);
                    } else {
                        //  新增设备画
                        createDeviceInfoList.add(record);

                    }
//           userId 改变，需要更新用户信息
                    if (!userIdFromDevice.equals(userIdFromEs)) {
                        needCheckUserInfoList.add(record);
                    } else {
                        normalDeviceInfoList.add(record);
                        logger.info("DeviceInfoESSink.updateHandleWithBuilder  deviceId:{},用户无更改：userId:{} ", deviceId, userIdFromDevice);
                    }

                } else {
                    normalDeviceInfoList.add(record);
                    logger.warn("DeviceInfoESSink.updateHandleWithBuilder 丢失用户id,deviceId:{}", deviceId);
                }
            });

            if (!CollectionUtils.isEmpty(needCheckUserInfoList)) {
                List<SinkRecord> searchUserList = new ArrayList<>(needCheckUserInfoList);
//                放入USER_ID 作为标示
                searchUserList.forEach(record -> {
                    Map<String, Object> keyValues = record.getKeyValues();
                    String userId = String.valueOf(keyValues.get(USER_ID));
                    keyValues.put(SEARCH_USER_ID, userId);
                });
                List<SinkRecord> searchUserResultList = doQuery_FromES(SDHZ_USER_INFO_REALTIME.getIndex(), SDHZ_USER_INFO_REALTIME.getType(), searchUserList, searchUserList.size());
//                组装了一个 userId ，userInfo 的map
                Map<String, Map<String, Object>> userInfoParamMap = new HashMap<>();
                for (SinkRecord sinkRecord : searchUserResultList) {
//                用户信息 map
                    Map<String, Object> keyValues = sinkRecord.getKeyValues();
                    Map<String, Object> userInfoMap = new HashMap<>();
                    Map<String, Object> userMap = (Map<String, Object>) keyValues.get(USER);
                    if (MapUtils.isEmpty(userMap)) {
                        continue;
                    }
                    for (String key : userMap.keySet()) {
                        if (!key.contains("es_metadata")) {
                            userInfoMap.put(key, userMap.get(key));
                        }
                    }
//         放入新的用户信息
                    userInfoParamMap.put("" + userInfoMap.get("id"), userInfoMap);
                    keyValues.remove(SEARCH_USER_ID);
                    keyValues.remove("old"+USER_ID);
                }
                for (SinkRecord record : needCheckUserInfoList) {
                    Map<String, Map<String, Object>> docMap = new HashMap<>(1);
                    Map<String, Object> keyValues = record.getKeyValues();
                    String deviceId = "" + keyValues.get("id");
                    keyValues.get(USER_ID);
                    keyValues.put(USER, userInfoParamMap.get("" + keyValues.get(USER_ID)));
                    docMap.put("doc", keyValues);
                    Update update = new Update.Builder(JSON.toJSONString(docMap)).id(deviceId).build();
                    bulkBuilder.addAction(update).build();
                    if (recordMap.containsKey(deviceId)) {
                        recordMap.get(deviceId).getKeyValues().putAll(keyValues);
                    } else {
                        recordMap.put(deviceId, record);
                    }
                }
            }

            if (!CollectionUtils.isEmpty(createDeviceInfoList)) {
                for (SinkRecord record : createDeviceInfoList) {
                    Map<String, Map<String, Object>> docMap = new HashMap<>(1);
                    Map<String, Object> keyValues = record.getKeyValues();
                    String deviceId = "" + keyValues.get("id");
                    docMap.put("doc", keyValues);
                    Update update = new Update.Builder(JSON.toJSONString(docMap)).id(deviceId).build();
                    bulkBuilder.addAction(update).build();
                    if (recordMap.containsKey(deviceId)) {
                        recordMap.get(deviceId).getKeyValues().putAll(keyValues);
                    } else {
                        recordMap.put(deviceId, record);
                    }
                }
            }

            if (!CollectionUtils.isEmpty(normalDeviceInfoList)) {
                for (SinkRecord record : normalDeviceInfoList) {
                    Map<String, Map<String, Object>> docMap = new HashMap<>(1);
                    Map<String, Object> keyValues = record.getKeyValues();
                    String deviceId = "" + keyValues.get("id");
                    docMap.put("doc", keyValues);
                    Update update = new Update.Builder(JSON.toJSONString(docMap)).id(deviceId).build();
                    bulkBuilder.addAction(update).build();
                    if (recordMap.containsKey(deviceId)) {
                        recordMap.get(deviceId).getKeyValues().putAll(keyValues);
                    } else {
                        recordMap.put(deviceId, record);
                    }
                }
            }
        }
        return recordMap;

    }

    @Override
    public JestResult afterUpdateProcess(Map<String, SinkRecord> recordMap, JestResult result) throws Exception {
//        这步仅针对设备画像更新做处理，
//        判断是否有更新user信息，更新的需要 insert 到 设备画像
//        如果是用户画像，不需要，直接跳过
        if (MapUtils.isEmpty(recordMap)) {
            return result;
        }
        Map<String, SinkRecord> deviceUpdateMap = new HashMap<>();
        for (Map.Entry<String, SinkRecord> entry : recordMap.entrySet()) {
            SinkRecord record = entry.getValue();
            if (null == record) {
                continue;
            }
            Map<String, Object> keyValues = record.getKeyValues();
            if (MapUtils.isEmpty(keyValues)) {
                continue;
            }
//            这里说明是用户更新的，map中只含有 id 和 user略过
            if (keyValues.size() == 2 && keyValues.containsKey("id") && keyValues.containsKey(USER)) {
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