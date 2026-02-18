package com.team44.isa_youtubeich.crdt;

import com.team44.isa_youtubeich.domain.model.Video;
import com.team44.isa_youtubeich.instance.InstanceIdLeaseService;
import com.team44.isa_youtubeich.repository.VideoRepository;
import com.team44.isa_youtubeich.repository.VideoViewRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.io.*;
import java.util.Base64;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class VideoCounter extends GCounter {

    private static final Logger log = LoggerFactory.getLogger(VideoCounter.class);

    private final long videoId;
    private final VideoViewRepository videoViewRepository;
    private final VideoRepository videoRepository;

    private final AtomicLong dbValue = new AtomicLong(0);
    private final AtomicLong dbTimestamp = new AtomicLong(Long.MAX_VALUE);

    public VideoCounter(StringRedisTemplate redisTemplate, InstanceIdLeaseService leaseService, RedisMessageListenerContainer container, String channel, long videoId, VideoViewRepository videoViewRepository, VideoRepository videoRepository) {
        super(redisTemplate, leaseService, container, channel);
        this.videoId = videoId;
        this.videoViewRepository = videoViewRepository;
        this.videoRepository = videoRepository;
    }

    @Override
    @PostConstruct
    public void init() {
        super.init();
        // Acquire initial db value
        Video video = videoRepository.findById(videoId).orElseThrow(() -> new RuntimeException("Video not found"));
        long localCount = videoViewRepository.countByVideo(video);
        long localTimestamp = System.currentTimeMillis();
        updateDbValue(localCount, localTimestamp);
        // Publish req
        publishReq(localTimestamp, localCount);
    }

    @Override
    public void increment() {
        UUID myId = getLeaseService().getInstanceId();

        // 1. Update local state first
        getCounters().computeIfAbsent(myId, k -> new AtomicLong()).incrementAndGet();

        broadcastCurrentState();
    }

    @Override
    public long getValue() {
        return super.getValue() + dbValue.get();
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        byte[] decoded = Base64.getDecoder().decode(message.getBody());
        if (decoded.length == 0) return;
        char type = (char) decoded[0];
        try (ByteArrayInputStream bais = new ByteArrayInputStream(decoded, 1, decoded.length - 1);
             DataInputStream dis = new DataInputStream(bais)) {

            if (type == 'i') {
                // Handle Full State Merge
                int mapSize = dis.readInt();
                for (int i = 0; i < mapSize; i++) {
                    UUID id = readUUID(dis);
                    long remoteValue = dis.readLong();

                    // Standard G-Counter Merge Rule: Max(local, remote)
                    getCounters().compute(id, (k, v) -> {
                        if (v == null) {
                            return new AtomicLong(remoteValue);
                        } else {
                            v.set(Math.max(v.get(), remoteValue));
                            return v;
                        }
                    });
                }
            } else if (type == 'r') {
                long reqTimestamp = dis.readLong();
                long reqValue = dis.readLong();
                publishRes(dbTimestamp.get(), dbValue.get());
                broadcastCurrentState();
            } else if (type == 's') {
                long resTimestamp = dis.readLong();
                long resValue = dis.readLong();
                if (resTimestamp < dbTimestamp.get() || (resTimestamp == dbTimestamp.get() && resValue > dbValue.get())) {
                    updateDbValue(resValue, resTimestamp);
                }
            }
        } catch (IOException e) {
            log.error("Failed to deserialize message", e);
        }
    }

    private void updateDbValue(long value, long timestamp) {
        dbValue.set(value);
        dbTimestamp.set(timestamp);
    }

    private void publishReq(long timestamp, long value) {
        publishMessage('r', timestamp, value);
    }

    private void publishRes(long timestamp, long value) {
        publishMessage('s', timestamp, value);
    }

    private void publishMessage(char type, long timestamp, long value) {
        byte[] message;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeByte(type);
            dos.writeLong(timestamp);
            dos.writeLong(value);
            message = baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize message", e);
        }

        try {
            String payload = Base64.getEncoder().encodeToString(message);
            getRedisTemplate().convertAndSend(getChannel(), payload);
        } catch (Exception e) {
            log.warn("Failed to publish message to Redis", e);
        }
    }

    private void broadcastCurrentState() {
        byte[] message;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            dos.writeByte('i'); // Type flag: Increment/State Update

            var currentMap = getCounters();
            dos.writeInt(currentMap.size()); // Write size

            // Serialize the full map
            for (var entry : currentMap.entrySet()) {
                writeUUID(dos, entry.getKey());
                dos.writeLong(entry.getValue().get());
            }

            message = baos.toByteArray();
        } catch (IOException e) {
            log.error("Failed to serialize state", e);
            return;
        }

        try {
            String payload = Base64.getEncoder().encodeToString(message);
            getRedisTemplate().convertAndSend(getChannel(), payload);
        } catch (Exception e) {
            log.warn("Failed to publish state to Redis", e);
        }
    }
}
