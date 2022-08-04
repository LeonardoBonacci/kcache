package guru.bonacci.kcache.example;

import org.springframework.context.ApplicationEvent;

public class SampleEvent extends ApplicationEvent {

  private static final long serialVersionUID = 1L;

  private String eventType;

  public SampleEvent(Object source, String key) {
    super(source);
    this.eventType = key;
  }
  
  public String getKey() {
    return eventType;
  }
}