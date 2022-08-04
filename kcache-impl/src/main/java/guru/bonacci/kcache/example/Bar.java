package guru.bonacci.kcache.example;

import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import io.kcache.KafkaCache;
import io.kcache.exceptions.CacheException;

@Component
public class Bar {

//  @Autowired KafkaCache<String, String> cache;
//  @Autowired KafkaTemplate<byte[], byte[]> producer;
//  @Autowired ApplicationEventPublisher applicationEventPublisher;
//  
//  @Transactional(transactionManager = "kafkaTransactionManager")
//  public String cacheAndProduce(String persistMe, boolean fail) {
//    String key = UUID.randomUUID().toString().substring(0, 6);
//    applicationEventPublisher.publishEvent(new RollbackEvent(this, key));
//
//    cache.put(key, persistMe);
//    if (fail) {
//      throw new RuntimeException("i should not happen");
//    }
//    ProducerRecord<byte[], byte[]> producerRecord = toRecord(key, persistMe, "please-consume-me");
//    producer.send(producerRecord);
//
//    return key;
//  }  
//
////  @TransactionalEventListener(phase = TransactionPhase.AFTER_ROLLBACK)
//  @TransactionalEventListener
//  public void rollBackOrder(RollbackEvent rollMeBack) {
//    System.out.println("roooooll");
//      cache.rollbackPut(rollMeBack.getKey());
//  }
//  
//  public ProducerRecord<byte[], byte[]> toRecord(String key, String value, String topic) {
//    ProducerRecord<byte[], byte[]> producerRecord;
//    try {
//        byte[] keyBytes = key == null
//            ? null : Serdes.String().serializer().serialize(topic, key);
//        byte[] valueBytes = value == null
//            ? null : Serdes.String().serializer().serialize(topic, value);
//        producerRecord = new ProducerRecord<>(topic, keyBytes, valueBytes);
//    } catch (Exception e) {
//        throw new CacheException("Error serializing key while creating the Kafka produce record", e);
//    }
//    return producerRecord;
//  }
//  
//  public static class RollbackEvent extends ApplicationEvent {
//    private String key;
//
//    public RollbackEvent(Object source, String key) {
//        super(source);
//        this.key = key;
//    }
//    public String getKey() {
//        return key;
//    }
//}
//  
}
