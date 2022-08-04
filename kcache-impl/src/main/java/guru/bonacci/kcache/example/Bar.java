package guru.bonacci.kcache.example;

import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import io.kcache.KafkaCache;
import io.kcache.exceptions.CacheException;

@Component
public class Bar {

  @Autowired KafkaCache<String, String> cache;
  @Autowired KafkaTemplate<byte[], byte[]> producer;
  @Autowired PlatformTransactionManager transactionManager;
  
  @Transactional(transactionManager = "kafkaTransactionManager")
  public String produceAndCache(String persistMe, boolean fail) {
    String key = UUID.randomUUID().toString().substring(0, 6);

    ProducerRecord<byte[], byte[]> producerRecord = toRecord(key, persistMe, "please-consume-me");
    producer.send(producerRecord);
    if (fail) {
      throw new RuntimeException("i should not happen");
    }
    cache.put(key, persistMe);

    return key;
  }  

  public String cacheAndProduce(String persistMe, boolean fail) {
    String key = UUID.randomUUID().toString().substring(0, 6);

    cache.put(key, persistMe);
    if (fail) {
      throw new RuntimeException("i should not happen");
    }
    ProducerRecord<byte[], byte[]> producerRecord = toRecord(key, persistMe, "please-consume-me");
    producer.send(producerRecord);

    return key;
  }  

  public ProducerRecord<byte[], byte[]> toRecord(String key, String value, String topic) {
    ProducerRecord<byte[], byte[]> producerRecord;
    try {
        byte[] keyBytes = key == null
            ? null : Serdes.String().serializer().serialize(topic, key);
        byte[] valueBytes = value == null
            ? null : Serdes.String().serializer().serialize(topic, value);
        producerRecord = new ProducerRecord<>(topic, keyBytes, valueBytes);
    } catch (Exception e) {
        throw new CacheException("Error serializing key while creating the Kafka produce record", e);
    }
    return producerRecord;
  }
}
