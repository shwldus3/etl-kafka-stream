package org.etl;

import java.io.InputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigUtil {
  private static Properties config = new Properties();

  public ConfigUtil() {
    try {
      InputStream inputStream = this.getClass().getResourceAsStream("/config.properties");
      config.load(inputStream);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String getProperty(String key) {
    return config.getProperty(key) == null ? "" : config.getProperty(key);
  }

}
