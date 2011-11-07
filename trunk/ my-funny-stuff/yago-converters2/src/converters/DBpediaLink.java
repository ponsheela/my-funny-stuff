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
import javatools.parsers.Char;

/**
 * Class DBpediaLink
 * 
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class produces the links between DBpedia and YAGO in the spirit of the
 * Linking Open data project. More precisely, it produces
 * 
 * <http://dbpedia.org/class/yago/class-name> owl:equivalentClass
 * <http://yago-knowldge.org/resource/class-name>
 * 
 * and
 * 
 * <http://dbpedia.org/resource/individual-name> owl:sameAs
 * <http://yago-knowledge.org/resource/individual-name>
 * 
 * @author Fabian M. Suchanek
 */
public class DBpediaLink extends Converter {

	/**
	 * starts to link DBpedia and YAGO
	 * 
	 * @throws Exception
	 */
	public void run() throws IOException {
		getParameters();
		Announce.doing("Linking DBpedia and YAGO");

		Collection<String> entities = new TreeSet<String>();
		int counter = 0;
		for (String line : new FileLines(new File(yagoFolder, "hasWikipediaUrl.tsv"),
				"Caching individuals")) {
			String[] split = line.split("\t");
			if (split[2].startsWith("yago"))
				continue;
			entities.add(split[1]);
			if (test && counter++ > 100)
				break;
		}

		Announce.doing("Writing individuals");
		Writer out = new BufferedWriter(new FileWriter(new File(outputFolder,
				"DBpediaYAGOlink_individuals.n3")));
		D.writeln(out, "@prefix owl:  <http://www.w3.org/2002/07/owl#> .");
		for (String name : entities) {
			name = Char.encodeURIPathComponent(Char.decodeBackslash(name));
			D.writeln(out, "<http://yago-knowledge.org/resource/" + name
					+ "> owl:sameAs <http://dbpedia.org/resource/" + name
					+ "> .");

		}
		out.close();
		Announce.done();

		entities.clear();
		counter = 0;
		for (String line : new FileLines(
				new File(yagoFolder, "subclassOf.tsv"), "Caching classes")) {
			if (test && counter++ > 100)
				break;
			String[] split = line.split("\t");
			if (split[2].startsWith("yago"))
				continue;
			entities.add(split[1]);
		}

		Announce.doing("Writing classes");
		out = new BufferedWriter(new FileWriter(new File(outputFolder,
				"DBpediaYAGOlink_classes.n3")));
		D.writeln(out, "@prefix owl:  <http://www.w3.org/2002/07/owl#> .");
		for (String name : entities) {
			name = Char.encodeURIPathComponent(Char.decodeBackslash(name));
			D.writeln(out, "<http://yago-knowledge.org/resource/" + name
					+ "> owl:sameAs <http://dbpedia.org/class/yago/" + name
					+ "> .");
		}
		out.close();
		Announce.done();

		Announce.done();
	}

	@Override
	public String description() {
		return ("Link YAGO to DBpedia");
	}
}
