package converters;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import test.SPOTLXConverterSplit;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.administrative.Parameters;
import basics.Basics;
import basics.Config;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class calls the appropriate class method for conversions to XML, RDFS,
 * N3 and SQL
 * 
 * @author Fabian M. Suchanek
 */
public abstract class Converter {

	/**
	 * set to true to run as test only top 100 of each file
	 */
	public boolean test = false;

	/**
	 * Does the conversion
	 * 
	 * @throws Exception
	 */
	public abstract void run() throws IOException, Exception;

	/** Returns info string */
	public abstract String description();

	/** the yago folder */
	public File yagoFolder;

	/**
	 * target output folder
	 */
	public File outputFolder;

	/** Tells whether the deductive closure shall be converted as well */
	public boolean deductiveClosure;
	
	/** Tells whether transitive facts should be converted */
	public boolean transitiveClosure;

	/** Tells whether facts about facts shall be included */
	public boolean includeFactFacts;
	
	/** Tells whether facts with arguments longer than 255 shall be included */
	public int maximumFactLength;
	
	/** The whitelist of relations - if this list is NOT NULL, only relations in this list will be imported 
	 * To fill this list, use the parameter 'importRelations' in the yago.ini, and a space-separated enumeration
	 * as value for the param */
	protected Set<String> importRelations = null;

	/** Returns the YAGO relation for a file name (or NULL)*/
	public String relationForFactFile(File inputFile) {
		if (inputFile.isDirectory()
				|| !inputFile.getName().endsWith(".tsv")
				|| inputFile.getName().startsWith("_")) 
			return(null);
				
		String relation = inputFile.getName();
		relation = relation.substring(0, relation.lastIndexOf('.'));
				
    // check if relation is excluded from import
    if (importRelations != null) {
      if (!importRelations.contains(relation)) {
        return null;
      }
    }
		
		if(!includeFactFacts && Basics.domain(relation).equals("yagoFact")) {
		 return(null);
		}
		
		if (!deductiveClosure && relation.equals("type_star")) {
			return(null);
		}

		if (!transitiveClosure && relation.endsWith("_transitive")) {
		  return null;
		}
		
		if (relation.endsWith("_transitive")) {
		  relation = relation.replace("_transitive", "");
		}
		
		if (relation.equals("type_star")) {
			relation = "type";
		}
		
    if(!Basics.isRelation(relation)) return(null);
    
		return(relation);
	}
	/**
	 * Gets the necessary parameters
	 * 
	 * @throws IOException
	 */
	public void getParameters() throws IOException {
		D.println("The information you enter here will be saved to",
				Parameters.iniFile.getAbsoluteFile());
		Config.init(Parameters.iniFile);
		yagoFolder = Parameters.getOrRequestFileParameter("yagoFactsFolder",
				"Please enter the folder with the YAGO facts (the YAGO2 dump you downloaded)\n   This must be YAGO in the default format (tab-separated id+triples, TSV files), unzipped");
		Parameters.add("yagoFactsFolder", yagoFolder.toString());
		outputFolder = Parameters.getOrRequestFileParameter(
				"converterOutputFolder",
				"Please enter the folder where the converted YAGO should go (this setting is ignored when importing YAGO into a database)");
		// Fabian: let's not store this, because the users may choose different parameters for different formats
		// Johannes: this is very annoying because you can never use nohup then - always need for interaction
		
		// get the optional parameter for importing only certain relations
		String relationList = Parameters.get("importRelations", "");
		
		if (!relationList.equals("")) {
		  String[] relations = relationList.split(" ");
		  importRelations = new HashSet<String>(Arrays.asList(relations));
		}
		  
		Parameters.add("converterOutputFolder", outputFolder.toString());
	}

	/** Gets parameters and the parameters deductiveClosure and includeFactFacts */
	public void getExtendedParameters() throws IOException {
		deductiveClosure = Parameters
				.getOrRequestAndAddBoolean(
						"deductiveClosure",
						"Would you like to add the deductive closure of the form\n"
								+ "(x, type ,c1), (c1, subclassof, c2) => (x, type, c2) ?");
    transitiveClosure = Parameters
    .getOrRequestAndAddBoolean(
        "transitiveClosure",
        "Would you like to add the transitive closure of the form\n"
            + "(x, r, y), (y, r, z) => (x, r, z)\n" +
            		"for transitive relations 'r' (like isLocatedIn) ?");
		includeFactFacts = Parameters.getOrRequestAndAddBoolean(
				"includeMetaFacts",
				"Do you want to include facts about facts ?");
    maximumFactLength = Parameters.getInt("maximumFactLength", 255);
	}

	/** Lists the converters */
	private static Converter[] options = new Converter[] { 
	    new SQLConverter(), new SPOTLXConverter(), new SPOTLXConverterSplit(), 
			new XMLConverter(), new RDFSConverter(), new N3Converter(),
			new DBpediaLink(), new DBpediaExport(), new AlchConverter(), 
			new JenaConverter() };

	/**
	 * Start the program, one can either provide a ini file or the choice. or
	 * all required parameters are requested.
	 */
	public static void main(String[] args) throws Exception {

		D.println("Welcome to the Yago ontology convertor\n");
		
		// Open ini file
		String initFile = args == null || args.length == 0 ? "yago.ini"
				: args[0];
		File f = new File(initFile);
		if (!f.exists()) {
			Announce.doing("Creating ini file", f.getAbsoluteFile());
			f.createNewFile();
			Announce.done();
		}
		Parameters.init(f);

		// Get choice
		int choice = -1;		
		String method = Parameters.get("convert", "none");
		for (int i = 0; i < options.length; i++) {
			if (options[i].getClass().getName().equalsIgnoreCase(method)) {
				choice = i;
				break;
			}
		}
		while (choice < 0) {
			D.println("Please choose from the following options:");
			for (int i = 0; i < options.length; i++)
				D.println((i + 1) + ". " + options[i].description());
			D.println("0. quit");
			D.pl("Your choice:> ");
			try {
				choice = Integer.parseInt(D.r().trim());
			} catch (Exception e) {
				choice = -1;
			}
			if (choice == 0) {
				D.println("Program ended");
				return;
			}
			choice=choice-1;
		}
		options[choice].run();
	}
}
