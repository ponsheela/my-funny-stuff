package converters;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class produces the N3 triples for the relations MEANS, TYPE, SUBCLASSOF
 * in order to support the import of these facts to DBpedia.
 * 
 * This code is intended as a boilerplate for the DBpedia team. The YAGO team
 * does not actively maintain this code and does not guarantee that the code
 * produces the desired output for DBpedia.
 * 
 * @author Fabian M. Suchanek
 */
public class DBpediaExport extends Converter {

	/**
	 * Creates a DBpedia identifier for a YAGO identifier. yagoPrefix is TRUE
	 * for the prefix "yago:" and FALSE for the prefix "dbpedia:"
	 */
	public static String dbpediaNameFor(String yagoName, boolean yagoPrefix,
			boolean asString) {
		if (asString)
			return (yagoName);
		yagoName = Char.encodeURIPathComponent(Char.decodeBackslash(yagoName));
		if (!yagoPrefix)
			return ("<http://dbpedia.org/resource/" + yagoName + ">");
		if (yagoName.startsWith("wordnet_"))
			yagoName = yagoName.substring(8);
		if (yagoName.startsWith("wikicategory_"))
			yagoName = yagoName.substring(13);
		String[] split = yagoName.split("_");
		StringBuilder result = new StringBuilder(
				"<http://dbpedia.org/class/yago/");
		for (String s : split)
			result.append(Char.upCaseFirst(s));
		result.append('>');
		return (result.toString());
	}

	/** Produces the N3 output for DBpedia */
	public void run() throws IOException {
		getParameters();
		Announce.doing("Exporting YAGO for DBpedia");
		Writer out = new FileWriter(new File(outputFolder,
				"DBpediaYAGOexport.n3"));
		D.writeln(out,
				"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .");
		D.writeln(out,
				"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .");
		for (String fileName : new String[] { "type.tsv", "subclassOf.tsv",
				"means.tsv" }) {
			String relation = fileName.substring(0, fileName.lastIndexOf('.'));
			boolean useDBpediaPrefix = !relation.equals("type");
			boolean invert = relation.equals("means");
			boolean asString = relation.equals("means");
			if (relation.equals("means"))
				relation = "rdfs:label";
	     if (relation.equals("type"))
	        relation = "rdf:type";
       if (relation.equals("subclassOf"))
         relation = "rdfs:subClassOf";  
			Announce.doing("Converting", relation);
			int counter = 0;
			for (String l : new FileLines(new File(yagoFolder,fileName), "Parsing " + fileName)) {
				if (test && counter++ > 100)
					break;
				String[] split = l.split("\t");
				String arg1 = dbpediaNameFor(split[invert ? 2 : 1],
						useDBpediaPrefix, false);
				String arg2 = dbpediaNameFor(split[invert ? 1 : 2], true,
						asString);
				out.write(arg1 + " " + relation + " " + arg2 + " .\n");
			}
			Announce.done();
		}
		out.close();
		Announce.done();
	}

	@Override
	public String description() {
		return ("Export YAGO data for DBpedia (unmaintained boilerplate code).");
	}

}
