package converters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.TreeSet;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.filehandlers.FileLines;
import javatools.filehandlers.UTF8Writer;
import javatools.parsers.Char;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class converts YAGO to the DB format required by Alchemy
 * 
 * @author Fabian M. Suchanek
 */
public class AlchConverter extends Converter {

	/**
	 * minimises the entity name
	 */
	public static String mlnNameFor(Object entity) {
		String name = entity.toString();
		if (name.startsWith("wikicategory_"))
			name = "cat_" + name.substring(13);
		if (name.startsWith("wordnet_"))
			name = "wn_" + name.substring(8);
		name = Char.upCaseFirst(Char.encodeHex(Char.decodeBackslash(name)));
		if (Character.isDigit(name.charAt(0)))
			name = "Y" + name;
		return (name);
	}

	/**
	 * creates the fact list
	 * 
	 * @param outputFolder
	 *            file
	 * @throws Exception
	 */
	public Collection<String> writeFacts() throws IOException {
		Announce.doing("Writing facts");
		Writer out = new BufferedWriter(new FileWriter(new File(outputFolder,
				"yago.db")));
		Collection<String> entities = new TreeSet<String>();
		for (File inputFile : yagoFolder.listFiles()) {
			String relation = relationForFactFile(inputFile);
			if (relation == null || relation.equals("type_star"))
				continue;
			entities.add(relation);

			relation = mlnNameFor(relation);
			int counter = 0;
			FileLines lines = new FileLines(inputFile, "Parsing "
					+ inputFile.getName());
			for (String line : lines) {
				String[] split = line.split("\t");
				entities.add(split[1]);
				entities.add(split[2]);
				D.writeln(out, "fact(" + mlnNameFor(split[1]) + ", " + relation
						+ ", " + mlnNameFor(split[2]) + ")");
				if (test && counter++ > 100)
					break;
			}
			lines.close();
		}
		out.close();
		Announce.done();
		return (entities);
	}

	@Override
	public void run() throws IOException {
		getParameters();
		includeFactFacts=false;
		Announce.doing("Producing Alchemy version of YAGO");

		// Write facts, get entities
		Collection<String> entities = writeFacts();

		// Write entities
		Writer entityWriter = new UTF8Writer(new File(outputFolder, "yago.mln"));
		D.writeln(entityWriter, "entity = {");
		for (String r : entities) {
			if (!r.startsWith("_"))
				entityWriter.write("  " + mlnNameFor(r) + ",\n");
		}
        D.writeln(entityWriter,"  dummy\n}\n");
        
		// Write rules
		D.writeln(entityWriter, "fact(entity, entity, entity)\n");
		D.writeln(entityWriter,
				"fact(x,Type,y) ^ fact(y,SubclassOf,z) => fact(x,Type,z).");
		D.writeln(entityWriter,
				"fact(r,HasDomain,d) ^ fact(x,r,y) => fact(x,Type,d).");
		D.writeln(entityWriter,
				"fact(r,HasRange,d) ^ fact(x,r,y) => fact(y,Type,d).");
		D
				.writeln(entityWriter,
						"100 fact(r,Type,YagoFunction) ^ fact(x,r,y) ^ fact(x,r,y2) => y=y2");
		entityWriter.close();

		Announce.done();
	}

	@Override
	public String description() {
		return ("Convert YAGO into the Markov Logic Network format used by Alchemy.");
	}

}
