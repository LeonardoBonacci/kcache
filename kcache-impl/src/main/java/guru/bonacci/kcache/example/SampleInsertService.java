package guru.bonacci.kcache.example;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class SampleInsertService{

     @Autowired
     private ApplicationEventPublisher applicationEventPublisher;

     public void someTransactionalMethod(boolean fail) {
          //Delete all call

          //Again Insert all calls

       applicationEventPublisher.publishEvent(new SampleEvent(this, "aaaaa"));
         if (fail) throw new RuntimeException();
          //Publish event after insert

          /** Some other call to DB which throws exception **/
     }

}