package guru.bonacci.kcache.example;

import org.apache.kafka.common.Uuid;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import io.kcache.KafkaCache;

@SpringBootApplication
@EnableTransactionManagement
public class Foo {

	public static void main(String[] args) {
		SpringApplication.run(Foo.class, args);
	}

//	@Bean
//  CommandLineRunner demo(KafkaCache<String, String> cache, Bar ppa) {
//    return args -> {
//      cache.init();
//
//      try {
//        String k = ppa.cacheAndProduce(Uuid.randomUuid().toString(), false);
//        System.out.println("!!!!!!!!!");
//        System.out.println("reading from cache " + cache.get(k));
//        System.out.println("!!!!!!!!!");
//      } catch (RuntimeException e) {
//        System.out.println(e.getLocalizedMessage());
//        System.out.println("!!!!!!!!!");
//      }
//
//      cache.all().forEachRemaining(kv -> System.out.println(kv.key + " with " + kv.value));
//      cache.close();
//      
//      Thread.sleep(4242);
//      System.out.println("bye");
//    };
//  }
	
  @Bean
  CommandLineRunner demo(SampleInsertService service) {
    return args -> {
      try {
        service.someTransactionalMethod(true);
      } catch(RuntimeException e) {}
      
      Thread.sleep(4242);
      System.out.println("bye");
    };
  }

//  @KafkaListener(topics = "please-consume-me", 
//                 groupId = "printer",
//                 properties = {"isolation.level:read_committed"})
//  public void listen(@Payload String message) {
//    System.out.println("incoming message... " + message);
//  }
}
