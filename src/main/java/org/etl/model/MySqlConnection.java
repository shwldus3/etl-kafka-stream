package org.etl.model;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.etl.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MySqlConnection {
  private DataSource dataSource;
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  public MySqlConnection() {
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

  public Connection getConnection() throws SQLException{
    return this.dataSource.getConnection();
  }
}
