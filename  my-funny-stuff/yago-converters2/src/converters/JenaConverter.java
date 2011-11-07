package converters;

import java.io.File;
import java.io.IOException;

import javatools.administrative.Announce;
import javatools.administrative.Parameters;
import javatools.datatypes.FinalMap;
import javatools.filehandlers.FileLines;
import javatools.parsers.Char;
import basics.Basics;
import basics.Config;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

import converters.RDFSConverter.RDFSDatatype;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) by the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class exports YAGO into Jena.
 * 
 * @author Fabian M. Suchanek
 */
public class JenaConverter extends Converter {

  /** Maps Yago relation names to their RDFS equivalent */
  public static FinalMap<String, Property> specialRelationNames = new FinalMap<String, Property>("means", RDFS.label, "type", RDF.type, "subclassOf",
      RDFS.subClassOf, "subpropertyOf", RDFS.subPropertyOf, "hasDomain", RDFS.domain, "hasRange", RDFS.range);

  /** Converts all facts to N3 */
  public void run() throws IOException {
    //test=true;
    try {
      Class.forName("com.hp.hpl.jena.tdb.TDBFactory");
    } catch (Exception e) {
      Announce.help("To import YAGO into Jena", "* download the Jena TDB from http://openjena.org/wiki/TDB",
          "* add all files in the TDB lib folder to the classpath", "* rerun this tool");
    }
    getParameters();
    includeFactFacts = false;
    deductiveClosure = false;
    File output = outputFolder;
    File[] files = output.listFiles();
    Announce.doing("Connecting to Jena TDB database", output);

    if (files != null) {
      Announce.progressStart("Deleting previous data", files.length);
      for (File f : files) {
        f.delete();
        Announce.progressStep();
      }
      Announce.progressDone();
    } else {
      output.mkdir();
    }
    // Create a special file to set the indexing strategy
    new File(output, "fixed.opt").createNewFile();
    Model model = TDBFactory.createModel(output.getPath());
    model.removeAll();
    model.setNsPrefixes(PrefixMapping.Extended);
    model.setNsPrefix("", RDFSConverter.ns);
    Announce.done();
    Parameters.add("jena", output.getPath());
    Announce.doing("Loading facts into Jena TDB database");
    //for (File inputFile : new PeekIterator.ElementaryPeekIterator<File>(new File("c:\\Fabian\\data\\yago\\data\\fact\\type.tsv"))) {//.listFiles()) {
    for (File inputFile : yagoFolder.listFiles()) {  
      String relation = relationForFactFile(inputFile);
      if (relation == null || relation.equals("isCalled") || relation.equals("during")) continue;
      RDFSDatatype objectType = RDFSConverter.rdfsTypeforYagoClass(Basics.range(relation));
      if (RDFSConverter.rdfsTypeforYagoClass(Basics.domain(relation)) != RDFSDatatype.rRESOURCE && objectType != RDFSDatatype.rRESOURCE) {
        continue;
      }
      boolean inverted = false;
      if (RDFSConverter.rdfsTypeforYagoClass(Basics.domain(relation)) != RDFSDatatype.rRESOURCE) {
        inverted = true;
        objectType = RDFSConverter.rdfsTypeforYagoClass(Basics.domain(relation));
      }
      Property relationName = specialRelationNames.containsKey(relation) ? specialRelationNames.get(relation) : model.createProperty(RDFSConverter.ns
          + relation);
      int counter = 0;
      for (String l : new FileLines(inputFile, "Parsing " + inputFile.getName())) {
        if (test && counter++ > 10) {
          Announce.progressDone();
          break;
        }
        String[] split = l.split("\t");
        if (split.length != 3) continue;
        String arg1 = Char.decodeBackslash(split[inverted ? 2 : 1]).replace('\\', '_');
        String arg2 = Char.decodeBackslash(split[inverted ? 1 : 2]).replace('\\', '_');
        Resource subject = model.createResource(RDFSConverter.ns + arg1);
        RDFNode object = null;
        switch (objectType) {
          case rboolean:
          case rdecimal:
          case rgYear:
          case rgeoCoordinate:
          case rnonNegativeInteger:
          case rdate:
          case rtimeInterval:
          case rdouble:
          case rduration:
          case rstring:
            object = model.createLiteral(basics.TermExtractor.stripQuotes(arg2));
            break;
          case rRESOURCE:
            object = model.createResource(RDFSConverter.ns + arg2);
            break;
        }
        model.add(subject, relationName, object);
      }
    }
    Announce.doing("Closing model");
    model.close();
    Announce.done();
    Announce.done();
  }

  @Override
  public String description() {
    return ("Load YAGO into a Jena TDB database\n      ----> use this option for SPARQL querying");
  }

  public static void main(String[] args) throws Exception {
    File iniFile = new File(args == null || args.length == 0 ? "yago.ini" : args[0]);
    Config.init(iniFile);
    new JenaConverter().run();
  }

}
