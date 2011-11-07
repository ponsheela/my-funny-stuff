package converters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char;

/**
 * Class GeoNamesLink
 * 
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class produces the links between GeoNames and YAGO in the spirit of the
 * Linking Open data project. More precisely, it produces
 * 
 * <http://sws.geonames.org/class/yago/class-name> owl:equivalentClass
 * <http://yago-knowledge.org/resource/class-name>
 * 
 * and
 * 
 * <http://sws.geonames.org/individual-id> owl:sameAs
 * <http://yago-knowledge.org/resource/individual-name>
 * 
 * @author Fabian M. Suchanek
 */
public class GeoNamesLink extends Converter {

	/**
	 * starts to link DBpedia and YAGO
	 * 
	 * @throws Exception
	 */
	public void run() throws IOException {
		getParameters();
		Announce.doing("Linking GeoNames and YAGO");

		Map<String, Integer> geoNamesIds = new HashMap<String, Integer>();
		int counter = 0;
		for (String line : new FileLines(new File(yagoFolder, "hasGeonamesId.tsv"),
				"Caching individuals")) {
			String[] split = line.split("\t");
			
			geoNamesIds.put(split[1], Integer.parseInt(split[2]));

			if (test && counter++ > 100)
				break;
		}

		Announce.doing("Writing individuals");
		Writer out = new BufferedWriter(new FileWriter(new File(outputFolder,
				"GeoNamesYAGOlink_individuals.n3")));
		D.writeln(out, "@prefix owl:  <http://www.w3.org/2002/07/owl#> .");
		for (String name : geoNamesIds.keySet()) {
			name = Char.encodeURIPathComponent(Char.decodeBackslash(name));
			D.writeln(out, "<http://yago-knowledge.org/resource/" + name
					+ "> owl:sameAs <http://sws.geonames.org/" + geoNamesIds.get(name)
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
