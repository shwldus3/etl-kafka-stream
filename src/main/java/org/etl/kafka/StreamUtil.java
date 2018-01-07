package org.etl.kafka;

import org.etl.model.Table;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class StreamUtil<T> {
  private Map<String, Object> dataMap = new HashMap();

  public String getQuery(T streamData) {
    Table t = new Table.Builder(streamData.getClass()).build();

    try {
      dataMap = getStreamData(streamData);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

    return t.getQuery(dataMap);
  }

  private static Map<String, Object> getStreamData(Object obj) throws IllegalAccessException{
    Map<String, Object> fieldMap = new HashMap<>();
    Class cls = obj.getClass();
    Field[] fields = cls.getDeclaredFields();

    for (Field field : fields) {
      String fieldName = field.getName();
      field.setAccessible(true);

      fieldMap.put(fieldName, field.get(obj));
    }
    return fieldMap;
  }

}
