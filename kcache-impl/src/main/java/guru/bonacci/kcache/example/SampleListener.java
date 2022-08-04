package guru.bonacci.kcache.example;

import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Component
public class SampleListener{

  @TransactionalEventListener(phase = TransactionPhase.AFTER_ROLLBACK)    
  public void handleSomeTransactionalEvent(SampleEvent event){
      System.out.println(event.getKey());
   }
}