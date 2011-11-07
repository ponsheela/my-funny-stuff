package converters;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javatools.administrative.Announce;
import javatools.administrative.D;
import javatools.administrative.Parameters;
import javatools.datatypes.PeekIterator;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.tdb.TDBFactory;

/**
 * This class is part of the YAGO converters (http://yago-knowledge.org). It is
 * licensed under the Creative Commons Attribution License
 * (http://creativecommons.org/licenses/by/3.0) the YAGO team
 * (http://yago-knowledge.org).
 * 
 * This class allows querying YAGO in SPARQL. For this purpose, YAGO has to
 * exist already in the Jena format (see readme.txt).
 * 
 * The basic usage is
 *    SPARQLprocessor qp=new SPARQLprocessor(jenaTDBdirectory);
 *    for(Map<String,RDFNode> solution : qp.query(QUERY)) {
 *          	// handle solution
 *    }
 *    // Other queries go here
 *    qp.close();
 * 
 * @author Fabian M. Suchanek
 */

public class SPARQLprocessor implements Closeable {

  /** Prefixes */
  public final String prefixes = "PREFIX : <" + RDFSConverter.ns + ">\n" + "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"
      + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>";

  /** Holds the Jena model*/
  protected Model model;

  /** Constructs a processor from a directory where the Jena version of YAGO lives*/
  public SPARQLprocessor(String jenaTDBdirectory) {
    Announce.doing("Connecting to Jena version of YAGO");
    model = TDBFactory.createModel(jenaTDBdirectory);
    // YAGO is too large for reasoning with RDFS...
    //model=ModelFactory.createRDFSModel(model);
    // YAGO is also too large to reason with Pellet...
    /*
    model=ModelFactory.createInfModel(PelletReasonerFactory.theInstance().create(), model);
        ValidityReport report = ((InfModel) model).validate();
        printIterator( report.getReports(), "Validation Results" );
        
        // print superclasses
        Resource c = model.getResource( RDFSConverter.ns+"#wikicategory_American_scientists");         
        printIterator(model.listObjectsOfProperty(c, RDFS.subClassOf), "All super classes of " + c.getLocalName());
        
        System.out.println();
        */
    Announce.done();
  }

  /** Frees resources*/
  public void close() {
    Announce.doing("Closing Jena version of YAGO");
    model.close();
    Announce.done();
  }

  /** returns variable bindings for a SPARQL query */
  public Iterable<Map<String, RDFNode>> query(String sparql, final int maxAnswers) {
    sparql = prefixes + "\n" + sparql;
    Query query = null;
    try {
      query = QueryFactory.create(sparql);
    } catch (RuntimeException e) {
      D.p(query);
      throw e;
    }
    final QueryExecution qexec = QueryExecutionFactory.create(query, model);
    final ResultSet results = qexec.execSelect();
    return (new PeekIterator<Map<String, RDFNode>>() {

      int counter = maxAnswers;

      @Override
      protected Map<String, RDFNode> internalNext() throws Exception {
        if (counter-- == 0 || !results.hasNext()) {
          qexec.close();
          return (null);
        }
        QuerySolution soln = results.nextSolution();
        Map<String, RDFNode> result = new TreeMap<String, RDFNode>();
        Iterator<String> vs = soln.varNames();
        while (vs.hasNext()) {
          String v = vs.next();
          result.put(v, soln.get(v));
        }
        return (result);
      }

    });
  }

  public static void main(String[] args) throws IOException {
    D.p("Welcome to the YAGO2 SPARQL processor!\n");
    String initFile = args == null || args.length == 0 ? "yago.ini" : args[0];
    File f = new File(initFile);
    if (!f.exists()) {
      Announce.doing("Creating ini file", f.getAbsoluteFile());
      f.createNewFile();
      Announce.done();
    }
    Parameters.init(new File(initFile));
    Parameters.getOrRequestAndAdd("jena",
        "Specify the path to the YAGO2 Jena TDB dump (either download from our website or create using yago2converters.sh)");
    String directory = Parameters.get("jena");
    SPARQLprocessor qp = new SPARQLprocessor(directory);
    D.p("\nTo know the relations that exist in YAGO, ask\n   SELECT ?r { ?r rdfs:domain ?x }");
    while (true) {
      D.p("\nEnter a SPARQL query, followed by a blank line (or a blank line to quit)");
      D.p("For example: ", "SELECT ?x WHERE { :Albert_Einstein rdf:type ?x }");
      String line = "";
      while (true) {
        String input = D.r().trim();
        if (input == null || input.length() == 0) break;
        line += "\n " + input;
      }
      if (line.length() == 0) break;
      try {
        for (Map<String, RDFNode> solution : qp.query(line, -1)) {
          D.p(solution);
        }
      } catch (Exception e) {
        D.p("Error:", e.getMessage());
      }
    }
    qp.close();
  }
}
