package com.arun.kafkaviewer.service;

import com.arun.kafkaviewer.api.KafkaMessage;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

final class PayloadCodec {

    private PayloadCodec() {
    }

    static KafkaMessage.Payload payload(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new KafkaMessage.Payload(asUtf8(bytes), Base64.getEncoder().encodeToString(bytes), bytes.length);
    }

    static KafkaMessage.MessageHeader header(String key, byte[] bytes) {
        if (bytes == null) {
            return new KafkaMessage.MessageHeader(key, null, null);
        }
        return new KafkaMessage.MessageHeader(
                key, asUtf8(bytes), Base64.getEncoder().encodeToString(bytes));
    }

    private static String asUtf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException ignored) {
            return null;
        }
    }
}
