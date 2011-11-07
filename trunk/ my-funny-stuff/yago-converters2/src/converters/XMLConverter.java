package converters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import javatools.administrative.Announce;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char;
import basics.TermExtractor;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class converts the Yago Ontology to XML
 * 
 * @author Gjergji Kasneci and Fabian M. Suchanek
 */
public class XMLConverter extends Converter {

	/**
	 * This method writes the corresponding dtd for the facts xml document as
	 * well as the corresponding facts xml file.
	 */
	public void run() throws IOException {
		getParameters();
		Announce.doing("Converting YAGO to XML");
		Writer xmlFactsWriter = new BufferedWriter(new FileWriter(new File(
				outputFolder, "xmlFacts.xml")));
		xmlFactsWriter.write("<?xml version=\"1.0\" ?>\n");
		xmlFactsWriter.write("<!DOCTYPE factset [\n");
		xmlFactsWriter.write("  <!ELEMENT factset   (fact*)>\n");
		xmlFactsWriter
				.write("  <!ELEMENT fact        (id, arg1, relation, arg2)>\n");
		xmlFactsWriter.write("  <!ELEMENT id         (#PCDATA)>\n");
		xmlFactsWriter.write("  <!ELEMENT relation   (#PCDATA)>\n");
		xmlFactsWriter.write("  <!ELEMENT arg1       (#PCDATA)>\n");
		xmlFactsWriter.write("  <!ELEMENT arg2       (#PCDATA)>\n");
		xmlFactsWriter.write("]>\n<factset>\n");
		String[] attributes = new String[] { "id", "arg1", "arg2" };
		for (File file : yagoFolder.listFiles()) {
			String relation = relationForFactFile(file);
			if(relation==null) continue;
			int counter = 0;
			for (String line : new FileLines(file, "Parsing " + file.getName())) {
				if (test && counter++ > 10) {
					Announce.progressDone();
					break;
				}
				String[] split = line.split("\t");
				xmlFactsWriter.write("  <fact>\n");
				for (int i = 0; i < attributes.length; i++) {
					if(i==2) xmlFactsWriter.write("   <relation>" + relation + "</relation>\n");
					xmlFactsWriter.write("   <" + attributes[i] + ">");
					xmlFactsWriter.write(Char.encodeAmpersand(Char
							.decodeBackslash(TermExtractor
									.stripQuotes(split[i]))));
					xmlFactsWriter.write("</" + attributes[i] + ">\n");
				}
				xmlFactsWriter.write("  </fact>\n");
			}
		}
		xmlFactsWriter.write("</factset>\n");
		xmlFactsWriter.close();
		Announce.done();
	}

	@Override
	public String description() {
		return ("Convert YAGO to XML");
	}

}
