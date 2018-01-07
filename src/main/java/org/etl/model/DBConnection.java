package org.etl.model;

import java.sql.*;
import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.etl.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DBConnection {
  private DataSource dataSource;
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  public DBConnection() {
    this.dataSource = init();
    logger.info("Create DB Connection Pool");
  }

  private DataSource init() {
    ConfigUtil config = new ConfigUtil();

    String dbUrl= config.getProperty("mysql.connectionString");
    String username = config.getProperty("mysql.username");
    String password = config.getProperty("mysql.password");

    ConnectionFactory connFactory = new DriverManagerConnectionFactory(dbUrl, username, password);

    GenericObjectPool connPool = new GenericObjectPool();
    connPool.setMaxActive(5);

    new PoolableConnectionFactory(connFactory, connPool, null, null, false, true);
    return new PoolingDataSource(connPool);
  }

  public void executeQuery(String query) {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = dataSource.getConnection();
      stmt = conn.createStatement();

      logger.info("DB => " + query);
      stmt.executeUpdate(query);

      stmt.close();
      conn.close();

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (stmt != null) stmt.close();
        if (conn != null) conn.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
