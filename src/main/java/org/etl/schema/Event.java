package org.etl.schema;

import java.sql.Timestamp;

public class Event {
  public long event_id;
  public Timestamp event_timestamp;
  public String service_code;
  public String event_context;

  public String toString() {
    return "Data => " + event_id + " | " + event_timestamp + " | " + service_code + " | " + event_context;
  }
}
