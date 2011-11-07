package converters;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import javatools.administrative.Announce;
import javatools.datatypes.FinalMap;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char;
import basics.Basics;
import basics.TermExtractor;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class produces the RDFS version of YAGO
 * 
 * @author Fabian M. Suchanek
 */
public class RDFSConverter extends Converter {

	/** Holds the YAGO namespace (has to be the URL where the YAGO Cool URI service runs) */
	public static final String ns = "http://yago-knowledge.org/resource/";

	/** All rdfs datatypes */
	public static enum RDFSDatatype {
		rboolean, rdouble, rdecimal, rnonNegativeInteger, rstring, rdate, rtimeInterval, rgYear, rduration, rgeoCoordinate, rRESOURCE;

		@Override
		public String toString() {
			return "\"&d;" + name().substring(1) + '"';
		}

		public String asLiteral(String literal) {
			return (Char.encodeAmpersand(Char.decodeBackslash(TermExtractor
					.stripQuotes(literal))));
		}
	};

	/** Maps Yago relation names to their RDFS equivalent */
	public static FinalMap<String, String> specialRelationNames = new FinalMap<String, String>(
			"means", "rdfs:label", "type", "rdf:type", "subclassOf",
			"rdfs:subClassOf", "subpropertyOf", "rdfs:subPropertyOf",
			"hasDomain", "rdfs:domain", "hasRange", "rdfs:range");

	/** Maps Yago classes to RDFS types */
	private static Map<String, RDFSDatatype> yago2rdfs = new FinalMap<String, RDFSDatatype>(
			"yagoBoolean", RDFSDatatype.rboolean, 
			"yagoNumber",	RDFSDatatype.rdouble,
			"yagoRationalNumber", RDFSDatatype.rdouble,
			"yagoInteger", RDFSDatatype.rdecimal,
			"yagoNonNegativeInteger",	RDFSDatatype.rnonNegativeInteger, 
			"yagoDate",	RDFSDatatype.rdate, 
			"yagoYear",	RDFSDatatype.rgYear,
			"yagoGeoCoordinatePair", RDFSDatatype.rgeoCoordinate,
			"yagoDuration", RDFSDatatype.rduration, 
			"yagoClass", RDFSDatatype.rRESOURCE, 
			"yagoFact", RDFSDatatype.rRESOURCE,
			"yagoLegalActor", RDFSDatatype.rRESOURCE, 
			"yagoLegalActorGeo", RDFSDatatype.rRESOURCE,
			"yagoGeoEntity", RDFSDatatype.rRESOURCE,
			"yagoFunction", RDFSDatatype.rRESOURCE,
			"yagoRelation", RDFSDatatype.rRESOURCE,
			"yagoSymmetricRelation", RDFSDatatype.rRESOURCE,
			"yagoTransitiveRelation", RDFSDatatype.rRESOURCE);

	/** Returns the RDFSType for a YagoClass (or RESOURCE if it's a resource) */
	public static RDFSDatatype rdfsTypeforYagoClass(String y) {
		if (yago2rdfs.containsKey(y))
			return (yago2rdfs.get(y));
		if (y.startsWith("yago"))
			return (RDFSDatatype.rstring);
		return (RDFSDatatype.rRESOURCE);
	}

	/** Formats a Yago entity as a URI */
	public static String asURI(String entity) {
		if (entity.startsWith("http://"))
			try {
				return (new URL(entity).toString());
			} catch (MalformedURLException e) {
				return ("\"http://malformed.example.com\"");
			}
		if (entity.startsWith("#"))
			return ("&f;fact_" + Char.encodeHex(entity));
		return ("&y;" + (Char.encodeURIPathComponent(entity).replace("&", "&amp;")));
	}

	/** Formats a Yago fact id for RDFS */
	public static String formatId(String entity) {
		entity = "fact_" + Char.encodeHex(entity);
		return (entity);
	}

	@Override
	public void run() throws IOException {
		getParameters();
		getExtendedParameters();
		Announce.doing("Converting facts to RDFS");

		Writer out = new BufferedWriter(new FileWriter(new File(outputFolder,
				"yago.rdfs")));
		out.write("<?xml version=\"1.0\"?>\n");
		out
				.write("<!DOCTYPE rdf:RDF [<!ENTITY d \"http://www.w3.org/2001/XMLSchema#\">\n");
		out.write("                   <!ENTITY y \"" + ns
				+ "\"> <!ENTITY f \"" + ns + "\">]>\n");
		out
				.write("<rdf:RDF xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n");
		out.write("         xml:base=\"" + ns + "\"\n");
		out
				.write("         xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"\n");
		out.write("         xmlns:y=\"" + ns + "\">\n");

		for (File inputFile : yagoFolder.listFiles()) {
			String relation = relationForFactFile(inputFile);
			if(relation==null) continue;
			RDFSDatatype objectType = rdfsTypeforYagoClass(Basics
					.range(relation));
			boolean inverted = false;
			if (rdfsTypeforYagoClass(Basics.domain(relation)) != RDFSDatatype.rRESOURCE) {
				inverted = true;
				objectType = rdfsTypeforYagoClass(Basics.domain(relation));
			}
			String relationName = specialRelationNames.containsKey(relation) ? specialRelationNames
					.get(relation)
					: "y:" + relation;
			boolean arg1isClass = inverted ? Basics.range(relation).equals(
					"yagoClass") : Basics.domain(relation).equals("yagoClass");
			int counter = 0;
			for (String l : new FileLines(inputFile, "Parsing "
					+ inputFile.getName())) {
				if (test && counter++ > 10) {
					Announce.progressDone();
					break;
				}
				String[] split = l.split("\t");
				if (split.length != 3) {
				  Announce.warning(inputFile + " has a non-well-formed line\n"+l);
				  continue;
				}
				String id = Char.decodeBackslash(split[0]);
				String arg1 = Char.decodeBackslash(split[inverted ? 2 : 1]);
				String arg2 = Char.decodeBackslash(split[inverted ? 1 : 2]);
				out.write(arg1isClass ? "<rdfs:Class" : "<rdf:Description");
				out.write(" rdf:about=\"" + asURI(arg1) + "\">");
				out.write("<" + relationName + " rdf:ID=\"" + formatId(id)
						+ "\"");
				if (objectType == RDFSDatatype.rRESOURCE) {
					out.write(" rdf:resource=\"" + asURI(arg2) + "\" />");
				} else {
					out.write(" rdf:datatype=" + objectType + " >");
					out.write(objectType.asLiteral(arg2));
					out.write("</" + relationName + ">");
				}
				out.write(arg1isClass ? "</rdfs:Class>\n"
						: "</rdf:Description>\n");
			}
		}
		out.write("</rdf:RDF>\n");
		out.close();
		Announce.done();
	}

	@Override
	public String description() {
		return ("Convert YAGO to RDFS");
	}
}
