package guru.bonacci.kcache.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.TransactionManager;

import io.kcache.KafkaCache;
import io.kcache.KafkaCacheConfig;

@Configuration
public class Config {

//	@Bean
//	public KafkaCache<String, String> kafkaCache() {
//	  return new KafkaCache<>(
//	      new KafkaCacheConfig(configProps()),
//	      Serdes.String(),  // for serializing/deserializing keys
//	      Serdes.String()   // for serializing/deserializing values
//	  );
//	}
//	
//  public Map<String, Object> configProps() {
//    String bootstrapServers = "localhost:9092";
//    
//    Map<String, Object> props = new HashMap<>();
//    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//    props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "i-am-unique");
//
//    return props;
//  }
//
//  @Bean
//  public ProducerFactory<byte[], byte[]> producerFactory() {
//    return new DefaultKafkaProducerFactory<>(configProps());
//  }
//
//  @Bean
//  public KafkaTemplate<byte[], byte[]> kafkaTemplate() {
//      return new KafkaTemplate<>(producerFactory());
//  }
//  
//  @Bean
//  public TransactionManager transactionManager(final ProducerFactory<byte[], byte[]> pf) {
//     return new KafkaTransactionManager<>(pf);
//  }
//  
//  @Bean
//  public NewTopic topic() {
//    return new NewTopic("please-consume-me", 1, (short) 1);
//  }
}
