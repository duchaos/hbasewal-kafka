package com.shuidihuzhu.transfer.sink.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

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


    @Override
    public void batchSink(List<SinkRecord> recordList) {
        try {
            Bulk.Builder bulkBuilder = new Bulk.Builder().defaultIndex(SDHZ_DEVICE_INFO_REALTIME.getIntex()).defaultType(SDHZ_DEVICE_INFO_REALTIME.getType());
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
    protected Map<String, Object> updateHandleWithBuilder(Map<String, Object> map) {
        String deviceId = String.valueOf(map.get(DEVICE_ID));
        Map<String, Object> userInfoMap = (Map<String, Object>) map.get("user");
//        不包含 userId 的 情况 ，直接返回更新的map
        if (MapUtils.isEmpty(userInfoMap) || !userInfoMap.containsKey("id")) {
            return map;
        }
        String userId = "";
        if (userInfoMap.containsKey("id")) {
            userId = String.valueOf(userInfoMap.get("id"));
        }
        if (StringUtils.isBlank(userId)) {
            return map;
        }
//        包含userId，根据userId 查询es
        try {
            JestResult jestResult = searchDocumentById(SDHZ_USER_INFO_REALTIME.getIntex(), SDHZ_USER_INFO_REALTIME.getType(), userId);
            userInfoMap = jestResult.getSourceAsObject(Map.class);
            map.put("user", JSON.toJSONString(userInfoMap));
        } catch (Exception e) {
            logger.error("DeviceInfoESSink.updateHandleWithBuilder", e);
        }
        return map;

    }
}