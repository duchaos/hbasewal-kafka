package com.ngdata.wal.ha;

import com.google.common.collect.Maps;
import com.google.common.io.FileWriteMode;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.ngdata.wal.service.ProducerCallback;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;

/**
 * @author duchao
 */
@Slf4j
@Component
public class FailMsgHandler {
    @Autowired
    private Producer<String, String> kafkaProducer;
    // 容错文件路径
    String path = "/data/retry";
    private StampedLock lock = new StampedLock();
    // 分隔符和换行符
    private static final String SPLIT_CHAR = "\001";
    private static final String POSTFIX = ".data";
    private static final String POSTFIXBAK = ".data.bak";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private Map<String, AtomicLong> topicAndFailCount = new ConcurrentHashMap<>();
    // 文件超过此阈值进行清理工作
    private long fileNumberThreshold = 5000;

    public FailMsgHandler() {
        File file = new File(path);
        if (!file.exists()) {
            if (file.mkdirs()) {
                log.info("create path: " + path);
            }
        }
    }

    private File initFile(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists() && file.createNewFile()) {
            log.info("createNewFile: " + filePath);
        }

        return file;
    }

    private void printFail(String topic) {
        log.warn("topicAndFailCount: " + topic + "=" + topicAndFailCount.get(topic));
    }

    private long getLineNumber(String path) throws IOException {
        try (LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(path))) {
            lineNumberReader.skip(Long.MAX_VALUE);
            // 实际上是读取换行符数量
            return lineNumberReader.getLineNumber();
        }
    }

    private long getMetadata(String topic) throws IOException {
        // 读取元数据信息
        String meta = Files.asCharSource(initFile(path + "/" + topic), StandardCharsets.UTF_8).readFirstLine();
        long position = 1;
        if (!StringUtils.isEmpty(meta)) {
            position = Long.parseLong(meta);
        }

        return position;
    }

    private String formatFileRowData(String topic, String key, String msg) {
        return topic + SPLIT_CHAR +
                key + SPLIT_CHAR +
                msg;
    }

    Map<String, AtomicLong> topicAndfileLineNumber = Maps.newConcurrentMap();

    public void append(String topic, String key, String msg) {
        long writeTims = lock.writeLock();
        String content = formatFileRowData(topic, key, msg);
        String dataFilePath = path + "/" + topic + POSTFIX;
        try {

            Files.asCharSink(initFile(dataFilePath), StandardCharsets.UTF_8, FileWriteMode.APPEND).write(content + LINE_SEPARATOR);
            recordFail(topic);
            if (topicAndfileLineNumber.containsKey(topic)) {
                topicAndfileLineNumber.computeIfPresent(topic, (k, v) -> new AtomicLong(v.incrementAndGet()));
            } else {
                topicAndfileLineNumber.computeIfAbsent(topic, k -> new AtomicLong(0));
            }
        } catch (Exception e) {
            log.error("记录发送失败消息[{}]到文件{}失败！！！ 原因:{}", content, dataFilePath, e);
        } finally {
            lock.unlock(writeTims);
        }
    }

    public boolean haveFail(String topic) throws IOException {
        boolean contains = topicAndFailCount.containsKey(topic);
        if (!contains) {
            return getLineNumber(path + "/" + topic) > 0;
        }
        return contains && topicAndFailCount.get(topic).get() > 0L;
    }

    private void recordFail(String topic) {
        // 记录错误的数量
        AtomicLong failCount = topicAndFailCount.computeIfAbsent(topic, k -> new AtomicLong(0));
        failCount.getAndIncrement();
    }

    private void writeMetadata(String topic, long position) {
        String metaPath = path + "/" + topic;
        try {
            Files.asCharSink(initFile(metaPath), StandardCharsets.UTF_8).write(String.valueOf(position));
        } catch (IOException e) {
            log.error("文件{}更新元数据失败！！！ 原因:{}", metaPath, e);
        }

        log.info("文件{}更新元数据成功，更新为：{}.", metaPath, position);
    }

    private void cleanFail(String topic) {
        topicAndFailCount.remove(topic);
    }

    private void cleanFile(String topic) {
        long start = System.currentTimeMillis();
        long writeTims = lock.writeLock();
        try {
            String topicPath = path + "/" + topic;
            File dataBakFile = initFile(topicPath + POSTFIXBAK);
            // 将文件置空
            Files.asCharSink(dataBakFile, StandardCharsets.UTF_8).write("");
            // 将未消费数据发送到bak文件
            Long lineNum = lineNumberMap.getOrDefault(topic, 0L);
            Long curNum = currNumberMap.getOrDefault(topic, 0L);
            long oldFileLineNumber = lineNum;
            lineNum = Files.asCharSource(initFile(topicPath + POSTFIX), StandardCharsets.UTF_8).readLines(new LineProcessorDataToBak(dataBakFile,topic));
            // 将bak重命名为数据文件
            if (!dataBakFile.renameTo(initFile(topicPath + POSTFIX))) {
                throw new Exception("renameTo fail!");
            }
            // 设置元数据
            curNum = 1L;
            writeMetadata(topic, curNum);
            log.info("cleanFile elapsedTime: {} ms, fileLineNumber is {} to {}", (System.currentTimeMillis() - start), oldFileLineNumber, lineNum);
        } catch (Exception e) {
            log.error("清理文件失败: " + e);
        } finally {
            lock.unlock(writeTims);
        }
    }

    private Map<String, Long> lineNumberMap = Maps.newConcurrentMap();
    private Map<String, Long> currNumberMap = Maps.newConcurrentMap();

    public void doRetrySend(String topic) throws Exception {
        log.info("doRetrySend start.");
        Long lineNum = lineNumberMap.getOrDefault(topic, 0L);
        Long curNum = currNumberMap.getOrDefault(topic, 0L);
        if (lineNum >= curNum) {
            // 如果文件行数过大则清理数据
            if (lineNum > fileNumberThreshold && curNum > 1) {
                cleanFile(topic);
            }
            // 按行读取数据，消费数据
            Files.asCharSource(initFile(path + "/topic" + POSTFIX), StandardCharsets.UTF_8).readLines(new LineProcessorRetrySend(topic));
            // 记录消费位置
            writeMetadata(topic, curNum);
            // 清理已完成的缓存
            cleanFail(topic);
            // 打印未完成的任务
            printFail(topic);
        }
    }

    private class LineProcessorRetrySend implements LineProcessor<Object> {
        long lineNumber = 0;
        long sendCont = 0;

        String topic;

        public LineProcessorRetrySend(String topic) {
            this.topic = topic;
        }

        @Override
        public boolean processLine(String line) throws IOException {
            // 更新元数据
            lineNumber++;
            Long curNum = currNumberMap.getOrDefault(topic, 0L);
            if (lineNumber >= curNum) {
                String[] rowData = line.split(SPLIT_CHAR);
                String topic = rowData[0];
                String key = rowData[1];
                String msg = rowData[2];
                // 尝试重新发送
                try {
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, msg);
                    kafkaProducer.send(producerRecord, new ProducerCallback(null, producerRecord));
                    // fail数-1
                    if (topicAndFailCount.containsKey(topic)) {
                        topicAndFailCount.get(topic).getAndDecrement();
                    }
                    sendCont++;
                    // 每发送累积到一些条数，记录消费位置
                    if (sendCont % 100 == 0) {
                        // 记录消费位置
                        writeMetadata(topic, curNum);
                    }
                    curNum = lineNumber + 1;
                    currNumberMap.put(topic, curNum);
                } catch (Exception e) {
                    log.error("doRetrySend error: " + e);
                    return false;
                }
            }

            return true;
        }

        @Override
        public Long getResult() {
            return null;
        }
    }

    private class LineProcessorDataToBak implements LineProcessor<Long> {
        long lineNumber = 0;
        long writeNum = 0;
        private Writer writer;
        String topic;

        private LineProcessorDataToBak(File file, String topic) throws IOException {
            this.writer = new FileWriter(file);
            this.topic = topic;
        }

        @Override
        public boolean processLine(String line) throws IOException {
            lineNumber++;
            Long curNum = currNumberMap.getOrDefault(topic, 0L);
            if (lineNumber >= curNum) {
                writer.write(line + LINE_SEPARATOR);
                writeNum++;
            }

            return true;
        }

        @Override
        public Long getResult() {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return writeNum;
        }
    }
}
