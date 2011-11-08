package converters;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.administrative.Parameters;
import javatools.database.MySQLDatabase;
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
public class SPOTLXConverter extends Converter {
	
	/** Batch size for bulk inserts */
	public final static int BATCH_SIZE = 1000;
	
	/** Connection to target database */
	private Connection targetConn;
	
	/** PreparedStatement for inserting relational (i.e., de-reified) facts */
	private PreparedStatement pstmtInsertRelationalFact;
	
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
	
	/** Calendar needed to account for timezone offsets when inserting timestamps */
	private static final Calendar cal = GregorianCalendar.getInstance(TimeZone.getTimeZone("GMT"));
	
	/** SQL statement for inserting relational facts */
	private final String insertRelationalFact = "INSERT INTO relationalfacts "
			+ "(id, relation, arg1, arg2, timeBegin, timeEnd, location, locationLatitude, locationLongitude, primaryWitness, context) " + "VALUES "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	
	/** Counts the number of not-yet-inserted but batched relational facts */
	private int batchCount;
	
	/** Initialize locations associated with entities 
	 * @throws IOException 
	 * @throws NumberFormatException */
	private void initializeGeoLocations() throws NumberFormatException, IOException {
		for (String l : new FileLines(new File(yagoFolder, "hasGeoCoordinates.tsv"), "Initializing geo-coordinates")) {
			String[] data = l.split("\t");
			String entity = data[1];
			String geoCoordinates = data[2];
			int pos = geoCoordinates.indexOf('/');
			double lat = Double.parseDouble(geoCoordinates.substring(0, pos));
			double lon = Double.parseDouble(geoCoordinates.substring(pos + 1, geoCoordinates.length()));
			geolocations.put(entity, new GeoLocation(lat, lon));
		}
		Announce.message(geolocations.size() + " geolocations (i.e., entities with known latitude and longitude)");
	}
	
	/** Initializes locations associated with facts 
	 * @throws IOException */
	private void initializeLocations() throws IOException {
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
	
	/** Initializes time intervals associated with facts 
	 * @throws IOException */
	private void initializeTimeIntervals() throws IOException {
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
	
	/** Initializes witnesses associated with facts 
	 * @throws IOException */
	private void initializeWitnesses() throws IOException {
		for (String l : new FileLines(new File(yagoFolder, "wasFoundIn.tsv"), "Initializing witnesses")) {
			String[] data = l.split("\t");
			String fact = data[1];
			String witness = data[2];
			
			witnesses.put(fact, witness);
		}
		Announce.message(witnesses.size() + " primary witnesses");
	}
	
	/** Initializes contexts associated with entities 
	 * @throws IOException */
	private void initializeContexts() throws IOException {
		String[] rels = new String[] { "hasWikipediaAnchorText", "hasWikipediaCategory", "hasCitationTitle" };
		
		Announce.doing("Initializing contexts");
		
		for (String relation : rels) {
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
	
	public void run() throws Exception {
		getParameters();
		getExtendedParameters();
		getDatabaseParameters();
		
		Announce.doing("Writing to Database");
		
		// load MySQL driver
		Driver driver = (Driver) Class.forName("com.mysql.jdbc.Driver").newInstance();
		DriverManager.registerDriver(driver);
		
		// source database connection
		String targetHost = Parameters.get("databaseHost");
		String targetPort = ":3306";
		String targetDatabase = Parameters.get("databaseDatabase");
		String targetUser = Parameters.get("databaseUser");
		String targetPW = Parameters.get("databasePassword");
		
		String targetUrl = "jdbc:mysql://" + targetHost + targetPort + (targetDatabase == null ? "" : "/" + targetDatabase);
		targetConn = DriverManager.getConnection(targetUrl, targetUser, targetPW);
		
		// configure Berkeley database environment (should be passed as a parameter)
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setReadOnly(false);
		envConfig.setTransactional(false);
		//            envConfig.setCacheSize(1024 * 1024 * 1024); // use 1G of memory
		envConfig.setCachePercent(50);
		
		File tempDir = outputFolder;
		
		// initialize mappings
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
		
		pstmtInsertRelationalFact = targetConn.prepareStatement(insertRelationalFact);
		
		Announce.doing("Creating table with following statement: " + "CREATE TABLE relationalfacts "
				+ "( id VARCHAR(255) NOT NULL, relation VARCHAR(255) NOT NULL, arg1 VARCHAR(255) NOT NULL, "
				+ "arg2 VARCHAR(255) NOT NULL, timeBegin TIMESTAMP, timeEnd TIMESTAMP, "
				+ "location VARCHAR(255), locationLatitude FLOAT, locationLongitude FLOAT, primaryWitness VARCHAR, context VARCHAR)");
		
		executeSQLUpdate("CREATE TABLE relationalfacts ( id VARCHAR(255) NOT NULL, "
				+ "relation VARCHAR(255) NOT NULL, arg1 VARCHAR(255) NOT NULL, arg2 VARCHAR(255) NOT NULL, "
				+ "timeBegin TIMESTAMP, timeEnd TIMESTAMP, location VARCHAR(255), " 
				+	"locationLatitude FLOAT, locationLongitude FLOAT, primaryWitness VARCHAR, context VARCHAR)");
		
		Announce.done();
		
		//
		// Initialize locations, time intervals, primary witnesses, and contexts
		//
		initializeGeoLocations();
		initializeLocations();
		initializeTimeIntervals();
		initializeWitnesses();
		initializeContexts();
		
		//
		// Perform one pass over facts and de-reify them
		//
		Announce.doing("Inserting facts");
		for (File f : yagoFolder.listFiles())
			loadFactsFrom(f);
				Announce.done();
				
				// process last batch
				execute();
				
				geolocationsDB.close();
				locationsDB.close();
				timeIntervalsDB.close();
				witnessesDB.close();
				contextsDB.close();
				dbEnv.close();
				
				// empty tempDir
				for (File f : tempDir.listFiles()) {
					f.delete();
				}
				
				pstmtInsertRelationalFact.close();
				
				Announce.doing("Computing Statistics");
				computeStatistics();
				Announce.done();
				
				Announce.doing("Creating Indexes");
				createIndexes(targetUrl, targetUser, targetPW);
				Announce.done();
				
				Announce.done();
	}
	
	private void createIndexes(String targetUrl, String targetUser, String targetPW) throws SQLException {
		Announce.message("Executing the following commands:");
		System.out.println("CREATE INDEX relationalfacts_id ON relationalfacts (id)");
		System.out.println("CREATE INDEX relationalfacts_id_arg1 ON relationalfacts (id, arg1)");
		System.out.println("CREATE INDEX relationalfact_relation_arg1_arg2 ON relationalfacts (relation, arg1, arg2)");
		System.out.println("CREATE INDEX relationalfacts_arg1_relation_arg2 ON relationalfacts (arg1, relation, arg2)");
		System.out.println("CREATE INDEX relationalfacts_arg2_relation_arg1 ON relationalfacts (arg2, relation, arg1)");
		System.out.println("CREATE INDEX relationalfacts_arg1_arg2_relation ON relationalfacts (arg1, arg2, relation)");
		System.out.println("CREATE INDEX relationalfacts_relation_arg2_arg1 ON relationalfacts (relation, arg2, arg1)");
		System.out.println("CREATE INDEX relationalfacts_arg2_arg1_relation ON relationalfacts (arg2, arg1, relation)");
		System.out.println("CREATE INDEX relationalfacts_spatial ON relationalfacts USING GIST(ST_GeographyFromText('SRID=4326;POINT(' || locationLongitude || ' ' || locationLatitude || ')'))");
		System.out.println("CREATE INDEX relationalfacts_timeBegin_timeEnd ON relationalfacts(timeBegin, timeEnd)");
		System.out.println("CREATE INDEX relationalfacts_timeEnd_timeBegin ON relationalfacts(timeEnd, timeBegin)");
		System.out.println("CREATE INDEX relationalfacts_location ON relationalfacts(location)");
		System.out.println("CREATE INDEX relationalfacts_locationLatitude_locationLongitude ON relationalfacts(locationLatitude, locationLongitude)");
		System.out.println("CREATE INDEX relationalfacts_locationLongitude_locationLatitude ON relationalfacts(locationLongitude, locationLatitude)");
		System.out.println("CREATE INDEX relationalfacts_arg1_text ON relationalfacts USING gin(to_tsvector('english', arg1))");
		System.out.println("CREATE INDEX relationalfacts_context_text ON relationalfacts USING gin(to_tsvector('english', context))");
		System.out.println("CREATE INDEX relationalfacts_arg1low_relation_arg2 ON relationalfacts (lower(arg1), relation, arg2)");
		System.out.println("CREATE INDEX relationalfacts_arg2low_relation_arg1 ON relationalfacts (lower(arg2), relation, arg1)");
		
		int numParallelIndexes = Integer.parseInt(Parameters.get("concurrentIndexes"));
		ExecutorService es = Executors.newFixedThreadPool(numParallelIndexes);
		
		IndexCreator ic1 = new IndexCreator("CREATE INDEX relationalfacts_id ON relationalfacts (id)", targetUrl, targetUser, targetPW);
		es.execute(ic1);    
		
		IndexCreator ic1a = new IndexCreator("CREATE INDEX relationalfacts_id_arg1 ON relationalfacts (id, arg1)", targetUrl, targetUser, targetPW);
		es.execute(ic1a);
		
		IndexCreator ic2 = new IndexCreator("CREATE INDEX relationalfact_relation_arg1_arg2 ON relationalfacts (relation, arg1, arg2)", targetUrl, targetUser, targetPW);
		es.execute(ic2);
		
		IndexCreator ic3 = new IndexCreator("CREATE INDEX relationalfacts_arg1_relation_arg2 ON relationalfacts (arg1, relation, arg2)", targetUrl, targetUser, targetPW);
		es.execute(ic3);
		
		IndexCreator ic4 = new IndexCreator("CREATE INDEX relationalfacts_arg2_relation_arg1 ON relationalfacts (arg2, relation, arg1)", targetUrl, targetUser, targetPW);
		es.execute(ic4);
		
		IndexCreator ic5 = new IndexCreator("CREATE INDEX relationalfacts_relation_arg2_arg1 ON relationalfacts (relation, arg2, arg1)", targetUrl, targetUser, targetPW);
		es.execute(ic5);
		
		IndexCreator ic6 = new IndexCreator("CREATE INDEX relationalfacts_arg1_arg2_relation ON relationalfacts (arg1, arg2, relation)", targetUrl, targetUser, targetPW);
		es.execute(ic6);
		
		IndexCreator ic7 = new IndexCreator("CREATE INDEX relationalfacts_arg2_arg1_relation ON relationalfacts (arg2, arg1, relation)", targetUrl, targetUser, targetPW);
		es.execute(ic7);
		
		IndexCreator ic8 = new IndexCreator("CREATE INDEX relationalfacts_spatial ON relationalfacts USING GIST(ST_GeographyFromText('SRID=4326;POINT(' || locationLongitude || ' ' || locationLatitude || ')'))", targetUrl, targetUser, targetPW);
		es.execute(ic8);
		
		IndexCreator ic9 = new IndexCreator("CREATE INDEX relationalfacts_timeBegin_timeEnd ON relationalfacts(timeBegin, timeEnd)", targetUrl, targetUser, targetPW);
		es.execute(ic9);
		
		IndexCreator ic10 = new IndexCreator("CREATE INDEX relationalfacts_timeEnd_timeBegin ON relationalfacts(timeEnd, timeBegin)", targetUrl, targetUser, targetPW);
		es.execute(ic10);
		
		IndexCreator ic11 = new IndexCreator("CREATE INDEX relationalfacts_location ON relationalfacts(location)", targetUrl, targetUser, targetPW);
		es.execute(ic11);
		
		IndexCreator ic12 = new IndexCreator("CREATE INDEX relationalfacts_locationLatitude_locationLongitude ON relationalfacts(locationLatitude, locationLongitude)", targetUrl, targetUser, targetPW);
		es.execute(ic12);
		
		IndexCreator ic13 = new IndexCreator("CREATE INDEX relationalfacts_locationLongitude_locationLatitude ON relationalfacts(locationLongitude, locationLatitude)", targetUrl, targetUser, targetPW);
		es.execute(ic13);
		
		IndexCreator ic14 = new IndexCreator("CREATE INDEX relationalfacts_arg1_text ON relationalfacts USING gin(to_tsvector('english', arg1))", targetUrl, targetUser, targetPW);
		es.execute(ic14);
		
		IndexCreator ic15 = new IndexCreator("CREATE INDEX relationalfacts_context_text ON relationalfacts USING gin(to_tsvector('english', context))", targetUrl, targetUser, targetPW);
		es.execute(ic15);
		
		IndexCreator ic16 = new IndexCreator("CREATE INDEX relationalfacts_arg1low_relation_arg2 ON relationalfacts (lower(arg1), relation, arg2)", targetUrl, targetUser, targetPW);
		es.execute(ic16);
		
		IndexCreator ic17 = new IndexCreator("CREATE INDEX relationalfacts_arg2low_relation_arg1 ON relationalfacts (lower(arg2), relation, arg1)", targetUrl, targetUser, targetPW);
		es.execute(ic17);
		
		try {
			es.shutdown();
			es.awaitTermination(14, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			es.shutdownNow();
			Announce.error(e);
		}
	}
	
	private void computeStatistics() throws SQLException {
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER id SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER arg1 SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER arg2 SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER relation SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER timeBegin SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER timeEnd SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER location SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER locationLatitude SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER locationLongitude SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER primaryWitness SET STATISTICS 1000");
		executeSQLUpdate("ALTER TABLE relationalfacts ALTER context SET STATISTICS 1000");
		executeSQLUpdate("VACUUM ANALYZE relationalfacts");
	}
	
	/** Adds facts from one file */
	public void loadFactsFrom(File file) throws Exception {
		
		// ignore files that are not relations
		String relation = relationForFactFile(file);
		if (relation == null) return;
		
		for (String l : new FileLines(file, "Parsing " + file)) {
			String[] data = l.split("\t");
			String id = data[0];
			String arg1 = data[1];
			String arg2 = data[2];
			
			if (relation.equals("hasGloss") && arg2.length() > maximumFactLength) {
				arg2 = arg2.subSequence(0, maximumFactLength-4) + "...\"";
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
			insert(id, relation, arg1, arg2, timeBegin, timeEnd, location, locationLatitude, locationLongitude, primaryWitness, context);
		}
	}
	
	private void refreschConnection() throws SQLException {
	// if the connection is closed, re-open it
			if (targetConn == null || targetConn.isClosed()) {
				D.pl("Connection is closed. Re-establishing it....");
				String targetHost = Parameters.get("databaseHost");
				String targetPort = ":3306";
				String targetDatabase = Parameters.get("databaseDatabase");
				String targetUser = Parameters.get("databaseUser");
				String targetPW = Parameters.get("databasePassword");
				
				String targetUrl = "jdbc:mysql://" + targetHost + targetPort + (targetDatabase == null ? "" : "/" + targetDatabase);
				targetConn = DriverManager.getConnection(targetUrl, targetUser, targetPW);
				D.pl("Connection re-established successfully");
			}
	}
	
	/** Adds relational (i.e., de-reified fact) to batch */
	private void insert(String id, String relation, String arg1, String arg2, long timeBegin, long timeEnd, String location, 
			double locationLatitude, double locationLongitude, String primaryWitness, String context) {
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
			if (locationLatitude != Double.NaN && locationLongitude != Double.NaN && Math.abs(locationLatitude) <= 90.0
					&& Math.abs(locationLongitude) <= 180.0) {
				pstmtInsertRelationalFact.setString(7, location);
				pstmtInsertRelationalFact.setDouble(8, locationLatitude);
				pstmtInsertRelationalFact.setDouble(9, locationLongitude);
			} else {
				pstmtInsertRelationalFact.setString(7, "");
				pstmtInsertRelationalFact.setNull(8, Types.FLOAT);
				pstmtInsertRelationalFact.setNull(9, Types.FLOAT);
			}
			
			if (primaryWitness != null && !context.equals("")) {
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
				refreschConnection();
				execute();
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
			//sqle.getNextException().printStackTrace();
			throw new RuntimeException(sqle);
		}
	}
	
	/** Insert current batch of relational facts into database */
	private void execute() {
		try {
			pstmtInsertRelationalFact.executeBatch();
			//      logger.info(batchCount + " de-reified facts inserted");
		} catch (SQLException sqle) {
			sqle.getNextException().printStackTrace();
			throw new RuntimeException(sqle);
		}
	}
	
	/** Executes the given SQL command 
	 * @throws SQLException */
	private void executeSQLUpdate(String sql) throws SQLException {
		Statement stmt = targetConn.createStatement();
		stmt.executeUpdate(sql);
		stmt.close();
	}
	
	@Override
	public String description() {
		return ("Read YAGO into a database in SPOTLX format");
	}
	
	private void getDatabaseParameters() throws IOException {
		javatools.database.Database database = new MySQLDatabase();
		
		// Parameters.add("databaseSystem", "postgres");
		
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
		Parameters.getOrRequestAndAdd("databaseDatabase", "Please enter the Postgres database");
		Parameters.getOrRequestAndAdd("concurrentIndexes", "How many Indexes should be created concurrently?");
	}
	
	public void testInsert() throws Exception {
		File file = new File("../yago2_data/type.tsv");
		getParameters();
		getExtendedParameters();
		getDatabaseParameters();
		
		Announce.doing("Writing to Database");
		
		// load MySQL driver
		Driver driver = (Driver) Class.forName("com.mysql.jdbc.Driver").newInstance();
		DriverManager.registerDriver(driver);
		
		loadFactsFrom(file);
	}
	
	public static void main(String[] args) throws Exception {
		File iniFile = new File(args == null || args.length == 0 || "test".equals(args[0]) ? "yago.ini" : args[0]);
		Config.init(iniFile);
		new SPOTLXConverter().run();
	}
}