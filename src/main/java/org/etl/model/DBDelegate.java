package org.etl.model;

public class DBDelegate {
  private DBService targetService;

  public DBDelegate(String target) {
    if (target.equals("MySQL")) {
      targetService = MySqlService.getInstance();
    } else {
      //TODO: You can add another kind of database.
    }
  }

  public DBService getTargetService() {
    return targetService;
  }
}
