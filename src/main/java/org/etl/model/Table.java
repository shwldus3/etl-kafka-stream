package org.etl.model;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.Map;
import java.util.ArrayList;

public class Table {
  private final String tableName;
  private final ArrayList<String> columnNames;

  public static class Builder {
    private String tableName;
    private ArrayList<String> columnNames = new ArrayList<String>();;

    public Builder (Class cls) {
      this.tableName = cls.getSimpleName();

      Field[] fields = cls.getDeclaredFields();

      for (Field field : fields) {
        this.columnNames.add(field.getName());
      }
    }

    public Table build() {
      return new Table(this);
    }
  }

  private Table(Builder builder) {
    tableName = builder.tableName;
    columnNames = builder.columnNames;
  }

  private int getColumnSize() {
    return this.columnNames.size();
  }

  private Object parseByType(Object data) {
    if (data instanceof String) return "\"" + data + "\"";
    else if (data instanceof Timestamp) return Timestamp.valueOf(data.toString()).getTime();
    else return data;
  }

  public String getQuery(Map dataMap) {
    int columnSize = getColumnSize();

    String query = "INSERT INTO " + tableName + " (";

    for (int i=0; i < columnSize; i++) {
      String name = columnNames.get(i);
      Object data = dataMap.get(name);
      if (data != null && !data.equals(""))
        query += (i == 0 ? "" : ", ") + name;
    }
    query += ") values (";
    for (int i=0; i < columnSize; i++) {
      String name = columnNames.get(i);
      Object data = dataMap.get(name);
      if (data != null && !data.equals(""))
        query += (i == 0 ? "" : ", ") + parseByType(data);
    }

    query += ")";

    return query;
  }
}
