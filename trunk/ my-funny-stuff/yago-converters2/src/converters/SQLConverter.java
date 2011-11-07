package converters;

import java.io.File;
import java.io.IOException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import javatools.administrative.Announce;
import javatools.administrative.Parameters;
import javatools.database.Database;
import javatools.database.Database.Inserter;
import javatools.database.PostgresDatabase;
import javatools.filehandlers.FileLines;
import basics.Config;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class inserts all text tables to an SQL Database.
 * 
 * @author Fabian M. Suchanek
 */
public class SQLConverter extends Converter {

  /** Holds the database */
  protected Database database = null;

  /** Table name */
  protected String factsTable = null;

  /** Special add-parameter for makeSQL */
  protected static final String INDEX = "INDEX";

  /** Adds facts from one file */
  public void loadFactsFrom(File file) throws Exception {
    String relation = relationForFactFile(file);
    if (relation == null) return;

    int[] columnTypes = { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR };

    Inserter bulki = database.new Inserter(factsTable, columnTypes);
    int counter = 0;
    for (String l : new FileLines(file, "Parsing " + file)) {
      if (test && counter++ > 100) break;
      String[] split = l.split("\t");
      String id = split[0];
      String arg1 = split[1];
      String arg2 = split[2];
      if (relation.equals("hasGloss") && arg2.length() > maximumFactLength) {
        arg2 = arg2.subSequence(0, maximumFactLength - 4) + "...\"";
      } else if (arg1.length() > maximumFactLength || arg2.length() > maximumFactLength) {
        continue;
      }
      try {
        bulki.insert(id, relation, arg1, arg2);
      } catch (Exception e) {
        System.err.println(e);
        return;
      }
    }
    bulki.close();
  }

  @Override
  public void getParameters() throws IOException {
    super.getParameters();
    getExtendedParameters();
  }

  @Override
  public void run() throws Exception {
    getParameters();
    database = Config.getDatabase();
    Announce.doing("Writing to database");
    
    factsTable = Parameters.get("databaseTable", "facts");

    boolean createTable = Parameters.getBoolean("createFactsTable", true);
    
    // Create facts table
    if (createTable) {
      Announce.doing("Creating "+factsTable+" table");
      if (database instanceof PostgresDatabase) {
        database.executeUpdate("CREATE TABLE "+factsTable+" (id VARCHAR, relation VARCHAR, arg1 TEXT, arg2 TEXT)");
      } else {
        database.executeUpdate("CREATE TABLE "+factsTable+" (id VARCHAR(255), relation VARCHAR(255), arg1 VARCHAR(255), arg2 VARCHAR(255))");
      }
      Announce.done();
    }

    Announce.doing("Inserting facts");
    for (File f : yagoFolder.listFiles())
      loadFactsFrom(f);
    Announce.done();
    
    Announce.doing("Creating indices on " + factsTable);
    List<String[]> indexes = null;
        
    int indexColumnCount = Parameters.getInt("indexColumnCount", 2);
    
    switch (indexColumnCount) {
      case 1:
        indexes = Arrays.asList(
            new String[] { "id" }, 
            new String[] { "id", "arg1" },
            new String[] { "relation" }, 
            new String[] { "arg1" }, 
            new String[] { "arg2" });
        break;

      case 2:
        indexes = Arrays.asList(
            new String[] { "id" }, 
            new String[] { "id", "arg1" },
            new String[] { "relation", "arg1" }, 
            new String[] { "arg1", "relation" }, 
            new String[] { "arg2", "relation" },
            new String[] { "arg1", "arg2" },
            new String[] { "relation", "arg2" }, 
            new String[] { "arg2", "arg1" });
        break;
        
      case 3:
        indexes = Arrays.asList(new String[] { "id" },
            new String[] { "id", "arg1" },
            new String[] { "relation", "arg1", "arg2" }, 
            new String[] { "arg1", "relation", "arg2" }, 
            new String[] { "arg2", "relation", "arg1" },
            new String[] { "arg1", "arg2", "relation" },
            new String[] { "relation", "arg2", "arg1" }, 
            new String[] { "arg2", "arg1", "relation" });
        break;
        
      default: // no indexes
        indexes = Arrays.asList(new String[] { "id" },
            new String[] { "id", "arg1" } );
        break;
    }
    
    // For large tables, MySQL uses a slow index building procedure, thus you can 
    // restrict the INDEX size by uncommenting the following. Otherwise see the 
    // README file for hints at MySQL index generation speedup or ask
    // Steffen Mezger for a way to build the usual indexes quickly
//    if (database instanceof MySQLDatabase) {
//      for (String[] l : indexes) {
//        for (int i = 0; i < l.length; i++)
//          l[i] += "(160)";
//      }
//    }
    
    if (database instanceof PostgresDatabase) {
      Announce.message("Statistics on the "+factsTable+" table are gathered");
      database.executeUpdate("ALTER TABLE "+factsTable+" ALTER id SET STATISTICS 1000");
      database.executeUpdate("ALTER TABLE "+factsTable+" ALTER relation SET STATISTICS 1000");
      database.executeUpdate("ALTER TABLE "+factsTable+" ALTER arg1 SET STATISTICS 1000");
      database.executeUpdate("ALTER TABLE "+factsTable+" ALTER arg2 SET STATISTICS 1000");
      database.executeUpdate("VACUUM ANALYZE "+factsTable+"");
    }
    Announce.done();
    
    Announce.message("The following indexes will be created:");
    for (String[] attr : indexes)
      Announce.message(database.createIndexCommand(factsTable, false, attr));
    Announce.message("On some systems, this fails. In these cases, please interrupt and create the indexes manually.");
    for (String[] attr : indexes) {
      Announce.doing(database.createIndexCommand(factsTable, false, attr));
      database.createIndex(factsTable, false, attr);
      Announce.done();
    }
    database.close();

    Announce.done();
  }

  @Override
  public String description() {
    return ("Read YAGO into a database\n      ----> use this option for SQL or native querying");
  }

  public static void main(String[] args) throws Exception {
    File iniFile = new File(args == null || args.length == 0 ? "yago.ini" : args[0]);
    Config.init(iniFile);
    new SQLConverter().run();
  }
}