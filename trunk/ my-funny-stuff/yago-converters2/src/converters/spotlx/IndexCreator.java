package converters.spotlx;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import javatools.administrative.Announce;

public class IndexCreator implements Runnable {

  private String sql;
  private String targetUrl;
  private String targetUser;
  private String targetPW;
 

  public IndexCreator(String indexCreationString, String targetUrl, String targetUser, String targetPW) throws SQLException {
    this.sql = indexCreationString;
    this.targetUrl = targetUrl;
    this.targetUser = targetUser;
    this.targetPW = targetPW;
  }

  @Override
  public void run() {
    Announce.message("[" + new Date() + "] Creating index: " + sql);
    try {
      executeSQLUpdate(sql);
    } catch (SQLException e) {
      Announce.error(e);
    }
    Announce.message("[" + new Date() + "] Done: " + sql);
  }

  private void executeSQLUpdate(String sql) throws SQLException {
    Connection con = DriverManager.getConnection(targetUrl, targetUser, targetPW);
    Statement stmt = con.createStatement();
    stmt.executeUpdate(sql);
    stmt.close();
    con.close();
  }
}
