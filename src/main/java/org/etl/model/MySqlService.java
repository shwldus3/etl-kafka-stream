package org.etl.model;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySqlService implements DBService {
  private final static MySqlConnection db = new MySqlConnection();
  private static MySqlService instance;
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  private MySqlService() { }

  public static MySqlService getInstance() {
    if (instance == null) {
      instance = new MySqlService();
    }

    return instance;
  }

  public void process(String query) {
    try {
      Connection conn = db.getConnection();
      Statement stmt = conn.createStatement();

      stmt.executeUpdate(query);
      logger.info("DB => " + query);

    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
