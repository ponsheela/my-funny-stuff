package test;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.administrative.Parameters;
import javatools.database.PostgresDatabase;
import javatools.filehandlers.FileLines;
import basics.Config;
import basics.Normalize;

import com.sleepycat.bind.tuple.StringBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import converters.Converter;
import converters.spotlx.DateParser;
import converters.spotlx.GeoLocation;
import converters.spotlx.IndexCreator;
import converters.spotlx.LocationBinding;
import converters.spotlx.TimeInterval;
import converters.spotlx.TimeIntervalBinding;

/**
 * 
 * @author Klaus Berberich (kberberi@mpi-inf.mpg.de)
 */
public class SPOTLXConverterSplit extends Converter {

  /** clears all existing tables */
  private boolean clearTables = false;

  private boolean onlyIndexes = false;

  /** Batch size for bulk inserts */
  public final static int BATCH_SIZE = 1000;

  /**
   * contains the info about which relation tables where created, so it can be
   * stopped and restarted at any time
   */
  private HashSet<String> tableCreated = null;

  /**
   * contains the info about which files where completed, so it can be stopped
   * and restarted at any time
   */
  private HashSet<String> filesDone = null;

  private final String filedoneName = "filedone";

  /** Connection to target database */
  private Connection targetConn;

  private String targetUser = null;

  private String targetPW = null;

  private String targetUrl = null;

  /** Calendar needed to account for timezone offsets when inserting timestamps */
  private static final Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("GMT"));

  /** Counts the number of not-yet-inserted but batched relational facts */
  private int batchCount = 0;

  /** Database environment */
  private Environment dbEnv;

  /** Maps entities to their associated geolocation */
  private Database geolocationsDB;

  private StoredMap<String, GeoLocation> geolocations;

  /** Maps facts to their associated location entity */
  private Database locationsDB;

  private StoredMap<String, String> locations;

  /** Maps facts to their associated time interval */
  private Database timeIntervalsDB;

  private StoredMap<String, TimeInterval> timeIntervals;

  /** Maps facts to their primary witnesses */
  private Database witnessesDB;

  private StoredMap<String, String> witnesses;

  /** Maps entities to their contexts */
  private Database contextsDB;

  private StoredMap<String, String> contexts;

  public void run() throws Exception {
    // initialize
    if (clearTables) {
      String input = D.read("All tables and temp files will be deleted, input 'y' to continue!");
      if (input == null || !"y".equals(input)) {
        return;
      }
    }
    init();
    if (clearTables) {
      Announce.doing("dropping all tables");
    } else if (onlyIndexes) {
      Announce.doing("Skipping inserting facts");
    } else {
      Announce.doing("Inserting facts");
    }
    if (!onlyIndexes) {
      // parse fact files
      for (File f : yagoFolder.listFiles()) {
        parseFiles(f);
      }
    }
    Announce.done();
    // create indexes
    if (!clearTables) {
      Announce.doing("creating indexes");
      HashSet<String> relationsDone = new HashSet<String>();
      for (File f : yagoFolder.listFiles()) {
        createStatsAndIndex(f, relationsDone);
      }
      Announce.done();
    }
    // finalyze
    close();
    Announce.message("END");
  }

  private void init() throws Exception {
    tableCreated = new HashSet<String>();
    getParameters();
    getExtendedParameters();
    getDatabaseParameters();
    // load PostgreSQL driver
    Driver driver = (Driver) Class.forName("org.postgresql.Driver").newInstance();
    DriverManager.registerDriver(driver);

    // source database connection
    String targetHost = Parameters.get("databaseHost");
    String targetDatabase = Parameters.get("databaseSchema");
    targetUser = Parameters.get("databaseUser");
    targetPW = Parameters.get("databasePassword");
    targetUrl = "jdbc:postgresql://" + targetHost + ":5432" + (targetDatabase == null ? "" : "/" + targetDatabase);
    targetConn = DriverManager.getConnection(targetUrl, targetUser, targetPW);

    // configure Berkeley database environment (should be passed as a parameter)
    EnvironmentConfig envConfig = new EnvironmentConfig();
    envConfig.setAllowCreate(true);
    envConfig.setReadOnly(false);
    envConfig.setTransactional(false);
    // envConfig.setCacheSize(1024 * 1024 * 1024); // use 1G of memory
    envConfig.setCachePercent(50);
    File tempDir = outputFolder;
    if (outputFolder.getAbsolutePath().equals(yagoFolder.getAbsolutePath())) {
      System.err.println("outputFolder cannot equal yagoFolder, exiting");
      System.exit(0);
    }
    // empty tempDir
    // initialize mappings
    if (!clearTables && !onlyIndexes) {
      StringBinding sb = (StringBinding) TupleBinding.getPrimitiveBinding(String.class);
      dbEnv = new Environment(tempDir, envConfig);
      DatabaseConfig dbConfig = new DatabaseConfig();
      dbConfig.setAllowCreate(true);
      dbConfig.setReadOnly(false);
      dbConfig.setTransactional(false);
      geolocationsDB = dbEnv.openDatabase(null, "geolocations", dbConfig);
      geolocations = new StoredMap<String, GeoLocation>(geolocationsDB, sb, new LocationBinding(), true);
      locationsDB = dbEnv.openDatabase(null, "locations", dbConfig);
      locations = new StoredMap<String, String>(locationsDB, sb, sb, true);
      timeIntervalsDB = dbEnv.openDatabase(null, "timeintervals", dbConfig);
      timeIntervals = new StoredMap<String, TimeInterval>(timeIntervalsDB, sb, new TimeIntervalBinding(), true);
      witnessesDB = dbEnv.openDatabase(null, "witnesses", dbConfig);
      witnesses = new StoredMap<String, String>(witnessesDB, sb, sb, true);
      contextsDB = dbEnv.openDatabase(null, "contexts", dbConfig);
      contexts = new StoredMap<String, String>(contextsDB, sb, sb, true);
      initializeGeoLocations();
      initializeLocations();
      initializeTimeIntervals();
      initializeWitnesses();
      initializeContexts();
    }
    filesDone = new HashSet<String>();
    if (clearTables) {
      dropTable(filedoneName);
      for (File f : tempDir.listFiles()) {
        f.delete();
      }
    } else {
      if (!tableExists(filedoneName)) {
        String tableSQL = "CREATE TABLE  " + filedoneName + " (name VARCHAR(255) NOT NULL)";
        executeSQLUpdate(tableSQL);
      } else {
        getDoneFiles();
      }
    }
  }

  private void close() throws Exception {
    if (!clearTables && !onlyIndexes) {
      geolocationsDB.close();
      locationsDB.close();
      timeIntervalsDB.close();
      witnessesDB.close();
      contextsDB.close();
      dbEnv.close();
    }
    // empty tempDir
    File tempDir = outputFolder;
    for (File f : tempDir.listFiles()) {
      f.delete();
    }
    dropTable(filedoneName);
    targetConn.close();
  }

  private void createStatsAndIndex(File file, HashSet<String> completedRelation) throws Exception {
    String relation = relationForFactFile(file);
    if (relation == null) return;
    if (completedRelation.contains(relation)) {
      return;
    } else {
      completedRelation.add(relation);
    }
    updateStats(relation);
    createIndex(relation);
  }

  private void parseFiles(File file) throws Exception {
    // ignore files that are not relations
    String relation = relationForFactFile(file);
    if (relation == null) return;
    if (!clearTables && filesDone.contains(file.getName())) {
      Announce.message("File has already been parsed " + file.getName());
      return;
    }
    // create Table
    dropTable(relation);
    if (!clearTables) {
      if (!tableExists(relation)) {
        String tableSQL = "CREATE TABLE  \"" + relation + "\" ( id VARCHAR(255) NOT NULL, relation VARCHAR(255) NOT NULL, arg1 VARCHAR(255) NOT NULL, arg2 VARCHAR(255) NOT NULL, "
            + "timeBegin TIMESTAMP WITH TIME ZONE, timeEnd TIMESTAMP WITH TIME ZONE, location VARCHAR(255), " + "locationLatitude FLOAT, locationLongitude FLOAT, primaryWitness VARCHAR, context VARCHAR)";
        executeSQLUpdate(tableSQL);
        tableCreated.add(relation);
      }
      parseFile(file, relation);
      String sql = "INSERT INTO " + filedoneName + " values ('" + file.getName() + "')";
      executeSQLUpdate(sql);
    }
  }

  /** checks if a table exist */
  private boolean tableExists(String relation) {
    boolean exists = true;
    Statement stmt = null;
    try {
      stmt = targetConn.createStatement();
      String sql = "select * from \"" + relation + "\" limit 1";
      ResultSet s = stmt.executeQuery(sql);
      s.close();
    } catch (Exception e) {
      exists = false;
    } finally {
      try {
        stmt.close();
      } catch (Exception e) {
      }
    }
    return exists;
  }

  /**
   * prepares the done file
   */
  private void getDoneFiles() {
    Statement stmt = null;
    try {
      stmt = targetConn.createStatement();
      String sql = "select * from " + filedoneName;
      ResultSet s = stmt.executeQuery(sql);
      while (s.next()) {
        String name = s.getString("name");
        filesDone.add(name);
        String relation = name.substring(0, name.lastIndexOf('.'));
        tableCreated.add(relation);
      }
      s.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        stmt.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * drops the table
   * 
   * @param relation
   * @throws Exception
   */
  private void dropTable(String relation) throws Exception {
    if (tableCreated.contains(relation)) {
      return;
    }
    if (tableExists(relation)) {
      String sql = "drop table \"" + relation + "\"";
      executeSQLUpdate(sql);
    }
  }

  /**
   * parses the file
   * 
   * @param file
   * @param relation
   * @throws Exception
   */
  private void parseFile(File file, String relation) throws Exception {
    Announce.doing("Parsing: " + relation);
    String insertFact = "INSERT INTO \"" + relation + "\" (id, relation,arg1, arg2, timeBegin, timeEnd, location, locationLatitude, locationLongitude, primaryWitness, context) VALUES (?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?)";
    PreparedStatement pstmtInsertRelationalFact = targetConn.prepareStatement(insertFact);
    for (String l : new FileLines(file)) {
      String[] data = l.split("\t");
      String id = data[0];
      String arg1 = data[1];
      String arg2 = data[2];
      if (relation.equals("hasGloss") && arg2.length() > maximumFactLength) {
        arg2 = arg2.subSequence(0, maximumFactLength - 4) + "...\"";
      } else if (arg1.length() > maximumFactLength || arg2.length() > maximumFactLength) {
        continue;
      }
      // determine time interval
      TimeInterval timeInterval = timeIntervals.get(id);
      long timeBegin = (timeInterval != null ? timeInterval.begin : TimeInterval.MIN_TIMESTAMP);
      long timeEnd = (timeInterval != null ? timeInterval.end : TimeInterval.MAX_TIMESTAMP);

      // determine location
      String location = locations.get(id);
      GeoLocation geoLocation = null;
      if (location != null) {
        geoLocation = geolocations.get(location);
      }

      double locationLatitude = (geoLocation != null ? geoLocation.latitude : Double.NaN);
      double locationLongitude = (geoLocation != null ? geoLocation.longitude : Double.NaN);

      // get primary witness of fact
      String primaryWitness = witnesses.get(id);

      // get context of subject and object
      String subjectContext = contexts.get(arg1);
      String objectContext = contexts.get(arg2);
      String context = (subjectContext != null ? subjectContext : "");
      context = (objectContext != null ? context + '\t' + objectContext : context);

      // add de-reified fact to batch
      insert(id, relation, arg1, arg2, timeBegin, timeEnd, location, locationLatitude, locationLongitude, primaryWitness, context, pstmtInsertRelationalFact);
    }
    execute(pstmtInsertRelationalFact);
    pstmtInsertRelationalFact.close();

    Announce.done();
  }

  /** Adds relational (i.e., de-reified fact) to batch */
  private void insert(String id, String relation, String arg1, String arg2, long timeBegin, long timeEnd, String location, double locationLatitude, double locationLongitude, String primaryWitness, String context,
      PreparedStatement pstmtInsertRelationalFact) {
    try {
      pstmtInsertRelationalFact.setString(1, id);
      pstmtInsertRelationalFact.setString(2, relation);
      pstmtInsertRelationalFact.setString(3, arg1);
      pstmtInsertRelationalFact.setString(4, arg2);

      // insert time interval -- only valid ones
      if (timeBegin <= timeEnd) {
        if (TimeInterval.MIN_TIMESTAMP < timeBegin && timeBegin < TimeInterval.MAX_TIMESTAMP) {
          pstmtInsertRelationalFact.setTimestamp(5, new Timestamp(timeBegin), cal);
        } else {
          pstmtInsertRelationalFact.setNull(5, Types.TIMESTAMP);
        }

        if (TimeInterval.MIN_TIMESTAMP < timeEnd && timeEnd < TimeInterval.MAX_TIMESTAMP) {
          pstmtInsertRelationalFact.setTimestamp(6, new Timestamp(timeEnd), cal);
        } else {
          pstmtInsertRelationalFact.setNull(6, Types.TIMESTAMP);
        }
      } else {
        pstmtInsertRelationalFact.setNull(5, Types.TIMESTAMP);
        pstmtInsertRelationalFact.setNull(6, Types.TIMESTAMP);
      }

      // insert location -- only valid ones
      if (locationLatitude != Double.NaN && locationLongitude != Double.NaN && Math.abs(locationLatitude) <= 90.0 && Math.abs(locationLongitude) <= 180.0) {
        pstmtInsertRelationalFact.setString(7, location);
        pstmtInsertRelationalFact.setDouble(8, locationLatitude);
        pstmtInsertRelationalFact.setDouble(9, locationLongitude);
      } else {
        pstmtInsertRelationalFact.setString(7, "");
        pstmtInsertRelationalFact.setNull(8, Types.FLOAT);
        pstmtInsertRelationalFact.setNull(9, Types.FLOAT);
      }

      if (primaryWitness != null && !primaryWitness.equals("")) {
        pstmtInsertRelationalFact.setString(10, primaryWitness);
      } else {
        pstmtInsertRelationalFact.setNull(10, Types.VARCHAR);
      }
      if (context != null && !context.equals("")) {
        pstmtInsertRelationalFact.setString(11, context);
      } else {
        pstmtInsertRelationalFact.setNull(11, Types.VARCHAR);
      }
      pstmtInsertRelationalFact.addBatch();
      if (++batchCount % BATCH_SIZE == 0) {
        execute(pstmtInsertRelationalFact);
      }
    } catch (SQLException sqle) {
      sqle.getNextException().printStackTrace();
      throw new RuntimeException(sqle);
    }
  }

  /** Insert current batch of relational facts into database */
  private void execute(PreparedStatement pstmtInsertRelationalFact) {
    try {
      pstmtInsertRelationalFact.executeBatch();
    } catch (SQLException sqle) {
      sqle.getNextException().printStackTrace();
      throw new RuntimeException(sqle);
    }
  }

  private void updateStats(String relation) throws Exception {
    Announce.doing("updating stats: " + relation);
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER id SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER arg1 SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER arg2 SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER timeBegin SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER timeEnd SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER location SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER locationLatitude SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER locationLongitude SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER primaryWitness SET STATISTICS 1000");
    executeSQLUpdate("ALTER TABLE \"" + relation + "\" ALTER context SET STATISTICS 1000");
    executeSQLUpdate("VACUUM ANALYZE \"" + relation + "\"");
    Announce.done();
  }

  private void createIndex(String relation) throws Exception {
    Announce.doing("creating indexes: " + relation);
    int numParallelIndexes = Integer.parseInt(Parameters.get("concurrentIndexes"));
    ExecutorService es = Executors.newFixedThreadPool(numParallelIndexes);

    String relationName = relation;
    if (relationName.length() > 30) {
      relationName = relationName.substring(0, 30);
    }
    String sql = null;
    sql = "CREATE INDEX " + relationName + "_id ON \"" + relation + "\" (id)";
    IndexCreator ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_arg1_arg2 ON \"" + relation + "\" (arg1, arg2)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_arg2_arg1 ON \"" + relation + "\" (arg2, arg1)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_spatial ON \"" + relation + "\" USING GIST(ST_GeographyFromText('SRID=4326;POINT(' || locationLongitude || ' ' || locationLatitude || ')'))";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_timeBegin_timeEnd ON \"" + relation + "\" (timeBegin, timeEnd)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_timeEnd_timeBegin ON \"" + relation + "\" (timeEnd, timeBegin)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_location ON \"" + relation + "\" (location)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_Latitude_Longitude ON \"" + relation + "\" (locationLatitude, locationLongitude)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_Longitude_Latitude ON \"" + relation + "\" (locationLongitude, locationLatitude)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_context_text ON \"" + relation + "\" USING gin(to_tsvector('english', context))";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_arg1low_arg2 ON \"" + relation + "\" (lower(arg1), arg2)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    sql = "CREATE INDEX " + relationName + "_arg2low_arg1 ON \"" + relation + "\" (lower(arg2), arg1)";
    ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
    es.execute(ic);
    // only for means
    if ("means".equals(relation)) {
      sql = "CREATE INDEX " + relationName + "_arg1_text ON \"" + relation + "\" USING gin(to_tsvector('english', arg1))";
      ic = new IndexCreator(sql, targetUrl, targetUser, targetPW);
      es.execute(ic);
    }
    try {
      es.shutdown();
      es.awaitTermination(14, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      es.shutdownNow();
      Announce.error(e);
    }
    Announce.done();
  }

  private void getDatabaseParameters() throws IOException {
    javatools.database.Database database = new PostgresDatabase();
    Parameters.add("databaseSystem", "postgres");
    if (!database.jarAvailable()) {
      D.pl("You are missing the JDBC jar file for this database. ");
      D.pl("You have to download it from the database provider and put it into the 'lib' directory in this package. ");
      D.pl("Then, restart the converter. ");
      D.pl("See README.txt for details.\n");
      D.exit();
    }
    Parameters.getOrRequestAndAdd("databaseUser", "Please enter the database user name");
    Parameters.getOrRequestAndAdd("databasePassword", "Please enter the database password");
    Parameters.getOrRequestAndAdd("databaseHost", "Please enter the database host");
    Parameters.getOrRequestAndAdd("databaseSchema", "Please enter the Postgres database schema");
  }

  @Override
  public String description() {
    return ("Read YAGO into a SPOTLX database where each relation has its own table");
  }

  /**
   * Executes the given SQL command
   * 
   * @throws SQLException
   */
  private void executeSQLUpdate(String sql) throws SQLException {
    Statement stmt = targetConn.createStatement();
    stmt.executeUpdate(sql);
    stmt.close();
  }

  /**
   * Initialize locations associated with entities
   * 
   * @throws IOException
   * @throws NumberFormatException
   */
  private void initializeGeoLocations() throws NumberFormatException, IOException {
    if (geolocations.size() > 0) {
      Announce.message(geolocations.size() + " geolocations (i.e., entities with known latitude and longitude)");
      return;
    }
    for (String l : new FileLines(new File(yagoFolder, "hasGeoCoordinates.tsv"), "Initializing geo-coordinates")) {
      String[] data = l.split("\t");
      String entity = data[1];
      String geoCoordinates = data[2];
      int pos = geoCoordinates.indexOf('/');
      double lat = Double.parseDouble(geoCoordinates.substring(1, pos));
      double lon = Double.parseDouble(geoCoordinates.substring(pos + 1, geoCoordinates.length() - 1));
      geolocations.put(entity, new GeoLocation(lat, lon));
    }
    Announce.message(geolocations.size() + " geolocations (i.e., entities with known latitude and longitude)");
  }

  /**
   * Initializes locations associated with facts
   * 
   * @throws IOException
   */
  private void initializeLocations() throws IOException {
    if (locations.size() > 0) {
      Announce.message(locations.size() + " locations");
      return;
    }
    String[] rels = new String[] { "occursIn" };
    Announce.doing("Initializing location entities");
    for (String relation : rels) {
      for (String line : new FileLines(new File(yagoFolder, relation + ".tsv"), "Initializing for " + relation)) {
        String[] data = line.split("\t");
        String fact = data[1];
        String location = data[2];
        locations.put(fact, location);
      }
    }
    Announce.message(locations.size() + " locations");
    Announce.done();
  }

  /**
   * Initializes time intervals associated with facts
   * 
   * @throws IOException
   */
  private void initializeTimeIntervals() throws IOException {
    if (timeIntervals.size() > 0) {
      Announce.message(timeIntervals.size() + " time intervals");
      return;
    }
    String[] rels = new String[] { "occursSince", "occursUntil" };
    Announce.doing("Initializing time intervals");
    for (String relation : rels) {
      for (String line : new FileLines(new File(yagoFolder, relation + ".tsv"), "Initializing for " + relation)) {
        String[] data = line.split("\t");
        String fact = data[1];
        String date = data[2];
        TimeInterval timeInterval = timeIntervals.get(fact);
        if (timeInterval == null) {
          timeInterval = new TimeInterval();
        }
        if (relation.equals("occursSince")) {
          long ms = DateParser.floorDate(date);
          timeInterval.setBegin(ms);
        } else if (relation.equals("occursUntil")) {
          long ms = DateParser.ceilDate(date);
          timeInterval.setEnd(ms);
        }
        timeIntervals.put(fact, timeInterval);
      }
    }
    Announce.message(timeIntervals.size() + " time intervals");
    Announce.done();
  }

  /**
   * Initializes witnesses associated with facts
   * 
   * @throws IOException
   */
  private void initializeWitnesses() throws IOException {
    if (witnesses.size() > 0) {
      Announce.message(witnesses.size() + " primary witnesses");
      return;
    }
    for (String l : new FileLines(new File(yagoFolder, "wasFoundIn.tsv"), "Initializing witnesses")) {
      String[] data = l.split("\t");
      String fact = data[1];
      String witness = data[2];
      witnesses.put(fact, witness);
    }
    Announce.message(witnesses.size() + " primary witnesses");
  }

  /**
   * Initializes contexts associated with entities
   * 
   * @throws IOException
   */
  private void initializeContexts() throws IOException {
    if (contexts.size() > 0) {
      Announce.message(contexts.size() + " contexts");
      return;
    }
    String[] rels = new String[] { "hasWikipediaAnchorText", "hasWikipediaCategory", "hasCitationTitle" };
    Announce.doing("Initializing contexts");
    for (String relation : rels) {
      File f = new File(yagoFolder, relation + ".tsv");
      if (!f.exists()) {
        Announce.message("missing file for " + relation);
        continue;
      }
      for (String line : new FileLines(new File(yagoFolder, relation + ".tsv"), "Initializing for " + relation)) {
        String[] data = line.split("\t");
        String entity = data[1];
        String context = contexts.get(entity);
        context = (context == null ? "" : context + '\t') + Normalize.unNormalize(data[2]);
        contexts.put(entity, context);
      }
    }
    Announce.message(contexts.size() + " contexts");
    Announce.done();
  }

  public static void main(String[] args) throws Exception {
    File iniFile = new File(args == null || args.length == 0 ? "yago.ini" : args[0]);
    Config.init(iniFile);
    new SPOTLXConverterSplit().run();
  }
}