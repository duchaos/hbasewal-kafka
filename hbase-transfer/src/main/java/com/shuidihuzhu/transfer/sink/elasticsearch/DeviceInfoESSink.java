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

    private static final String DEVICE_ID = "device_id";
    private static final String USER = "user";
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
     * 2.2 更新失败 FIXME  貌似用不着
     * *       根据设备id ，获取对应设备画像全部数据，内存中进行目标字段更新替换，放入用户画像部分信息，进行index
     *
     * @author: duchao01@shuidihuzhu.com
     * @date: 2019-06-21 18:44:47
     */
    private void updateDeviceFail() {
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
//       获取的是用户画像传来的用户id
        Object idObj = map.get("id");
        Object deviceIdObj = map.get(DEVICE_ID);
        String deviceId = String.valueOf(deviceIdObj);
        Map<String, Object> userInfoMap = (Map<String, Object>) map.get("user");
        if (null != deviceIdObj && StringUtils.isNotBlank(deviceId)) {
//           deviceId有效，说明用户画像更新调用,需要替换后续index 的id
            map.put("id", deviceId);
//            用户画像带来的信息，都是属于user 的
            map.remove(DEVICE_ID);

//        不包含 userId 的 情况 ，不作处理；
//        其中，也有可能是用户画像掉的，返回更新用户信息的map
            if (MapUtils.isEmpty(userInfoMap) || !userInfoMap.containsKey("id")) {
                HashMap<String, Object> hashMap = new HashMap<>(2);
                hashMap.put(USER, map);
                map.put("id", idObj == null ? "" : String.valueOf(idObj));
                hashMap.put("id", deviceId);
                return hashMap;
            }
        }
        if (MapUtils.isEmpty(userInfoMap)) {
            return map;
        }

        String userId = "";
        if (userInfoMap.containsKey("id")) {
            userId = String.valueOf(userInfoMap.get("id"));
            if (StringUtils.isBlank(userId)) {
                return map;
            }
        }
//        包含userId，根据userId 查询es
        try {
            JestResult jestResult = searchDocumentById(SDHZ_USER_INFO_REALTIME.getIntex(), SDHZ_USER_INFO_REALTIME.getType(), userId);
            userInfoMap = jestResult.getSourceAsObject(Map.class);
            map.put("user", userInfoMap);
        } catch (Exception e) {
            logger.error("DeviceInfoESSink.updateHandleWithBuilder", e);
        }
        return map;
    }

    @Override
    public JestResult batchInsertAction(Map<String, SinkRecord> rejectedRecordMap) throws Exception {
        /** 更新设备画像的时候，存在一个问题，那就是 如果设备换了用户id，那么用户所有信息都需要变更为新的
         *  目前能做到 替换设备信息中的新数据，需要确保新旧用户信息的字段一一对应，否则会出现旧数据的遗留
         * FIXME*/
        return batchInsertAction(rejectedRecordMap, bulkBuilder);
    }
}