package org.etl.schema;

import org.etl.model.Table;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class FormatConverter<T> {
  private Map<String, Object> dataMap = new HashMap();

  public FormatConverter() { }

  public String getQuery(T tClass) {
    Table t = new Table.Builder(tClass.getClass()).build();

    dataMap = parseSchemaValue(tClass);

    return t.getQuery(dataMap);
  }

  public Map<String, Object> parseSchemaValue(T tClass) {
    Map<String, Object> fieldMap = new HashMap<>();
    Class cls = tClass.getClass();
    Field[] fields = cls.getDeclaredFields();

    for (Field field : fields) {
      String fieldName = field.getName();
      field.setAccessible(true);

      try {
        fieldMap.put(fieldName, field.get(tClass));
      } catch (IllegalAccessException e) {
        e.printStackTrace();
      }
    }
    return fieldMap;
  }
}
