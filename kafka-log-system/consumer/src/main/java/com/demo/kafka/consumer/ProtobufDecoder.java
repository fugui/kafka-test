package com.demo.kafka.consumer;

import com.demo.kafka.common.proto.LogMessageProto.LogMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Protobuf 二进制解码器。
 * 将 Kafka 消息的 byte[] 载荷解码为 LogMessage 对象。
 * 解码在 Worker 线程中执行，不占用 Fetcher 线程时间。
 */
public class ProtobufDecoder {
    private static final Logger log = LoggerFactory.getLogger(ProtobufDecoder.class);

    /**
     * 解码二进制载荷为 LogMessage。
     *
     * @param payload Kafka 消息的 value (Protobuf 序列化的 byte[])
     * @return 解码后的 LogMessage
     * @throws InvalidProtocolBufferException 如果数据损坏无法解码（毒丸消息）
     */
    public LogMessage decode(byte[] payload) throws InvalidProtocolBufferException {
        if (payload == null || payload.length == 0) {
            throw new InvalidProtocolBufferException("Empty or null payload");
        }
        return LogMessage.parseFrom(payload);
    }
}
