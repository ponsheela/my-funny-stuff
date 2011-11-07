package converters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.administrative.Parameters;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char;
import basics.Basics;
import basics.TermExtractor;
import converters.RDFSConverter.RDFSDatatype;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class exports YAGO into N3.
 * 
 * @author Fabian M. Suchanek
 */
public class N3Converter extends Converter {

	/** Creates a N3 string */
	public static String n3String(String string) {
		string = TermExtractor.stripQuotes(string);
		return ('"' + Char.encodeBackslash(string) + '"');
	}

	/** Creates a N3 name */
	public static String n3NameFor(String string) {
		return ('<'+Char.encodeURIPathComponent(Char.decodeBackslash(string))+'>');
	}

	/** Converts all facts to N3 */
	public void run() throws IOException {
		getParameters();
		deductiveClosure = Parameters
				.getOrRequestAndAddBoolean(
						"deductiveClosure",
						"Would you like to add the deductive closure of the form\n"
								+ "(x,type,c1), (c1, subclassof, c2) => (x, type, c2) ?");
		includeFactFacts = false;

		Writer out = new BufferedWriter(new FileWriter(new File(outputFolder,
				"yago.n3")));
		D.writeln(out,
				"@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .");
		D.writeln(out,
				"@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .");
		D.writeln(out, "@prefix y: <"+RDFSConverter.ns+"> .");
		D.writeln(out, "@prefix x: <http://www.w3.org/2001/XMLSchema#> .");
		D.writeln(out, "@base <"+RDFSConverter.ns+"> .");

		Announce.doing("Converting facts to N3");
		for (File inputFile : yagoFolder.listFiles()) {
			String relation = relationForFactFile(inputFile);
			if(relation==null) continue;
			RDFSDatatype objectType = RDFSConverter.rdfsTypeforYagoClass(Basics
					.range(relation));
			if (RDFSConverter.rdfsTypeforYagoClass(Basics.domain(relation)) != RDFSDatatype.rRESOURCE && objectType!=RDFSDatatype.rRESOURCE) {
				continue;
			}
			boolean inverted = false;
			if (RDFSConverter.rdfsTypeforYagoClass(Basics.domain(relation)) != RDFSDatatype.rRESOURCE) {
				inverted = true;
				objectType = RDFSConverter.rdfsTypeforYagoClass(Basics
						.domain(relation));
			}
			String relationName = RDFSConverter.specialRelationNames
					.containsKey(relation) ? RDFSConverter.specialRelationNames
					.get(relation) : "y:" + relation;
			int counter = 0;
			for (String l : new FileLines(inputFile, "Parsing "
					+ inputFile.getName())) {
				if (test && counter++ > 10) {
					Announce.progressDone();
					break;
				}
				String[] split = l.split("\t");
        if (split.length != 3) {
          Announce.error(inputFile + " is not well-formed, stopping conversion");
          Announce.done();
          System.exit(1);
        }
				String arg1 = Char.decodeBackslash(split[inverted ? 2 : 1]);
				String arg2 = Char.decodeBackslash(split[inverted ? 1 : 2]);
				out.write(n3NameFor(arg1) + " " + relationName + " ");
				switch (objectType) {
				case rboolean:
				case rdecimal:
				case rgYear:
				case rnonNegativeInteger:
				case rdate:
				case rtimeInterval:
				case rdouble:
				case rgeoCoordinate:
				case rduration:
					out.write('"' + TermExtractor.stripQuotes(arg2) + '"');
					break;
				case rRESOURCE:
					out.write(n3NameFor(arg2));
					break;
				case rstring:
					out.write(n3String(arg2));
					break;
				}
				out.write(" .\n");
			}
		}
		Announce.done();
		out.close();
	}

	@Override
	public String description() {
		return ("Convert YAGO to N3 (Notation 3)");
	}

}
