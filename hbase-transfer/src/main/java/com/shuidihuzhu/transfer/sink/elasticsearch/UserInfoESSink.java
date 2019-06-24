package com.shuidihuzhu.transfer.sink.elasticsearch;

import com.shuidihuzhu.transfer.enums.TransferEnum;
import com.shuidihuzhu.transfer.model.SinkRecord;
import io.searchbox.client.JestResult;
import io.searchbox.core.Bulk;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

import static com.shuidihuzhu.transfer.enums.TransferEnum.SDHZ_USER_INFO_REALTIME;

/**
 * @author duchao
 * @version : UserInfoESSink.java, v 0.1 2019-06-21 15:03 duchao Exp $
 */
@Service
public class UserInfoESSink extends ESSink {

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
                deviceInfoESSink.batchUpdateAction(recordList, syncBuild);
            }
        } catch (Exception e) {
            logger.error("UserInfoESSink.batchSink into es error.", e);
            handleBatchErrorRecord(recordList);
        }
    }

}