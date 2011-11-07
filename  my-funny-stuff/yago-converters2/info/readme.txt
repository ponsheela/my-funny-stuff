               The YAGO Converters
          Johannes Hoffart, Fabian M. Suchanek, 
          Gjergji Kasneci, Edwin Lewis-Kelham
             This version: 2010-11-22

------------------ Introduction ---------------------------------------

This ZIP file contains the Java-programs to convert YAGO to different
formats.

This code is licensed under the Creative Commons Attribution License
(http://creativecommons.org/licenses/by/3.0/) by the YAGO team
(http://yago-knowledge.org). It contains the Jena and the Berkeley DB Java library.
See below for the licenses.

If you use YAGO for scientific purposes, please cite our paper:
    Fabian M. Suchanek, Gjergji Kasneci and Gerhard Weikum
    "YAGO - A Core of Semantic Knowledge"
    16th international World Wide Web conference (WWW 2007)
    PDF: http://www.mpii.de/~suchanek/publications/www2007.pdf
    BIB: http://www.mpii.de/~suchanek/publications/www2007.bib


------------------ What you can do ------------------------------------

You should first download the YAGO ontology itself in its native form from
  http://www.yago-knowledge.org

Then the YAGO code allows you to do one of the following
* Convert YAGO to RDFS (also available online)
* Convert YAGO to XML
* Convert YAGO to N3 (Notation 3, also available online)
* Read YAGO into a database (MySQL, Oracle, Postgres)
* Read YAGO into a Postgres database using the new SPOTLX data model
* Link YAGO and DBpedia by owl:sameAs statements
  (also available online)
* Export YAGO's class hierarchy for importing it into DBpedia
  (This code is intended as a boilerplate for the DBpedia team. The YAGO
   team does not actively maintain this code and does not guarantee that
   the code produces the desired output for DBpedia)
* Export YAGO into Alchemy
* Import YAGO into a Jena TDB (also available online)
* Query YAGO 
  - through SPARQL with Jena 
  - through a Database
  - in its native form

------------------ What you need --------------------------------------

You need the Java Runtime-Environment to run the YAGO programs. Java
can be downloaded here:
    http://java.sun.com

If you want to 
* read YAGO into a database
* query YAGO in the native query language
* query YAGO in SQL
* query YAGO in SPOTLX (Postgres only)
then you need a database. We support Oracle, Postgres and MySQL.
You need the Java libraries that establish the connection between 
Java and the database system (the JDBC Jar-files). These are database 
version specific and bound by licenses, so that we cannot ship them.
* For Oracle, see whether the Jar-file is already there in your version of
  the database. It should be something like
            classes12.jar or ojdbc14.jar 
  If it is not, download the JDBC driver from
            http://www.oracle.com/
* For Postgres, the JDBC file can be downloaded here:
     http://jdbc.postgresql.org/
* For MySQL, the JDBC file can be downloaded here:
     http://www.mysql.org/
     -> Downloads -> Connectors -> Connectors/J
Simply copy the jar file with the JDBC driver to the lib/ directory
in this package, and it will be added to the classpath when running
the shell/batch script automatically.
        
If you want to query YAGO in SPOTLX you need a PostGIS enabled Postgres database.
You can get the plugin at http://postgis.refractions.net/.
The converters will take care of creating the table, but PostGIS
needs to be enabled for the database first:
  createlang plpgsql yago2spotlx_db 
  psql -d yago2spotlx_db -f postgis.sql 
  psql -d yago2spotlx_db -f spatial_ref_sys.sql
For more details on this, see here: 
http://postgis.refractions.net/documentation/manual-1.5/ch02.html#id2630392
        
        
------------------ yago.ini -------------------------------------------

When you run the Java programs, they will prompt you for information
such as folder names and user names. The Java programs store this information
in a file called yago.ini so that you do not need to retype it the next time.
In case you want to change this information, you can modify or delete this file.

There are optional parameters that will not be prompted, but will be
used if set in the yago.ini file:
* oneColumnIndexes = yes|no
	Instead of creating all triple indexes which are needed for efficient
	querying of the database, create only indexes on single columns


------------------ Converting YAGO -----------------------------

YAGO can be converted to a wide variety of output formats (see above
"What you can do"). In particular, if you want to query YAGO, you have 
to convert it:
* If you want to query in SPARQL, you have to convert YAGO into Jena
  (or use the downloaded YAGO in Jena format from our site)
* If you want to query in YAGO's native format (including meta facts),
  or in SQL, then you have to read YAGO into a database
* If you want to query YAGO in SPOTLX, you need to read it into a
  database in the SPOTLX format.
    
Here is how it works: Start the YAGO2 converters by typing

    yago2converters.bat
    
  Under UNIX, it has to be
  
    ./yago2converters.sh
    
  IMPORTANT: If you are using a database (i.e. if you import YAGO2 in a DB), 
             you need the appropriate JDBC driver (in the form of a jar) for
             the database you are using. See above under "What you need".
               
The programs will prompt you for all information needed. All programs 
can also be run with the path to another yago.ini-file as argument.

HINT: Any conversion is likely to take hours. Run it in a screen.
The database converters, in particular, will build indexes for all permutations 
of pairs of arg1-relation-arg2, i.e. it will build 6 indexes for these three 
columns. This takes a while, depending on your machine, even up to a week. 
If you want to create indexes for single columns only, add the paramater
'indexColumnCount' to the yago.ini, and set it to 1.
If you want to have even more performance when querying the database, 
especially when using joins, you can also set the paramater to 3, which will 
then build 6 indexes with all permutations of arg1-relation-arg2.

------------------ Querying YAGO -----------------------------
	       
There are four ways to query YAGO
* In native YAGO query format (including meta facts)
  You should first load YAGO into a database as explained above. 
  Then run
  
       ./yago2query.sh
       
     or
     
       yago2query.bat
       
  If you want to query by Java API, use
      basics.QueryProcessor
      
* In SPARQL format
  You should first load YAGO into Jena as explained above, if you
  do not have downloaded YAGO in the Jena TDB format already.
  
  Once you have YAGO in Jena TDB format, run
  
       ./yago2sparql.sh 
       
     or 
     
       yago2sparql.bat
       
  If you want to query by Java API, use
      converters.SPARQLprocessor

* In SQL
  You should first load YAGO into a database as explained above.
  Then, you can access all facts in YAGO, including the meta facts,
  through standard SQL queries

* In SPOTLX
  You should first load YAGO into a Postgres database as explained
  above. Then, the SPOTLX data is available for querying in SQL in
  the database.

------------------ MySQL Bug --------------------------------------

Some people have reported that reading YAGO into MySQL hangs. In 
this case, first check whether there is enough space on your hard 
drive.

If there is enough space, we suggest the following: Interrupt the
program when the creation of indexes hangs. YAGO is already
completely in your database. Then you can create the indices
manually from within MySQL if you wish. We recommend
        CREATE INDEX i1 ON facts(id);
        CREATE INDEX i2 ON facts(arg1);
        CREATE INDEX i3 ON facts(arg2);
        CREATE INDEX i4 ON facts(relation);
        CREATE INDEX i5 ON facts(relation,arg1);
        CREATE INDEX i6 ON facts(relation,arg2);
        CREATE INDEX i7 ON facts(arg1,arg2);
        
You might also try to generate the longer index versions:
  CREATE INDEX factsidIndex ON facts (id);
  CREATE INDEX factsidarg1Index ON facts (id, arg1);
  CREATE INDEX factsrelationarg1arg2Index ON facts (relation, arg1, arg2);
  CREATE INDEX factsarg1relationarg2Index ON facts (arg1, relation, arg2);
  CREATE INDEX factsarg2relationarg1Index ON facts (arg2, relation, arg1);
  CREATE INDEX factsarg1arg2relationIndex ON facts (arg1, arg2, relation);
  CREATE INDEX factsrelationarg2arg1Index ON facts (relation, arg2, arg1);
  CREATE INDEX factsarg2arg1relationIndex ON facts (arg2, arg1, relation);
        
Some of these indexes might already be there.

There can be various reasons why index generation appears 
stuck (or is very slow) depending on your server configuration; 
We cannot provide detailed guidance on index generation for all 
cases and all database types that MySQL offers.
However, one of the major problems we encountered on the default 
MyISAM databases was that MySQL used the slow 'repair by keycache' 
method to build the indices instead of the 'repair by sort' method.
You can check which method the MySQL server uses by running the 
query "Show Processlist" repeatedly to observe the actions taken 
during index creation. Some parameters you might want to adjust 
to achieve a quicker indexing are:
- 'myisam_max_sort_file_size': This is the maximal file size
the 'repair by sort' method will use to handle temporary index
files. If the generated index is larger than this, it will go 
for the slower 'repair by keycache' method. The default setting 
is 2GB (your linux distribution might have set a larger default
value tough). The indices for YAGOs facts table are larger than 2 GB.
Hence, you should set this to a value as large as you have free space
on your harddisk. You can check your current setting by issuing the 
query 'SHOW VARIABLES'. Also, MySQL >estimates< how big the index
will be and if several indices are generated at once, it seems to add 
up the estimates and apparently sometimes tends to overestimate. While 
it generally speeds up the process to create all indices in one go, 
in some cases it might help to create them one by one to avoid a
fallback to the keycache method as the resulting size might be 
estimated too large to handle by sorting.
   
Furthermore: 
- 'myisam_sort_buffer_size' and 'sort_buffer_size': several pages 
on the web suggest setting them to ~30% of your RAM to improve 
index repair time with the 'repair by sort' method
- 'myisam_repair_threads': If you have a multi-core machine, you might 
want to increase the threads used for index repairs up to the numbers 
of cores you have. (Note that this feature is in 'beta' status, but 
seemed to perform well and without any problems in our trials).

Still, keep in mind that the YAGO facts table is quite large and 
thus the index generation will take some time even if MySQL is doing
the best it can. As a rough estimate, running the converter tools,
i.e. generation of the facts table including generation of the full 
indices step-by-step, took roughly around 9 hours on a decent desktop 
machine(2.4 Ghz quad-core) using 'repair by sort' with no other 
tuning (i.e. 1 repair thread, small default sized sort buffers).
 
One more hint regarding Collations: The converter tools will use the 
default collation of the server when creating the YAGO table. 
This is by default latin_swedish_ci, which means queries on VARCHAR 
columns are case-independent (except if you use the BINARY keyword). 
Some tools you might want to use on the YAGO tables might assume 
queries to be either case sensitive or case insensitive. 
Hence, you should make sure you set the default collation 
to a setting that you are comfortable with before running the converter 
tool, as changing the collation later on will trigger a regeneration 
of the indices.
Note however, that with some case sensitive collations, for example 
'utf8_bin', the size of a single entry of a full index, i.e. three
VARCHAR(255)s together, can become too large for MySQL to handle, 
such that it will fail to generate those indices.
 
Another hint is that, when creating indexes it is important that there 
is enough space in the partition where the tmpdir resides. Otherwise 
keycache reparing is used which may take ages.

------------------ Data Formats --------------------------------------

The data format of the converter output is as follows:
* RDFS:
  The output folder will contain a file yago.rdfs
  Since YAGO makes use of reification, each RDFS triple is given an ID.
  This id is used as an argument in other facts.
  The relation "isCalled" is translated to "rdfs:label" statements with
  the appropriate language tag.

* N3:
  The output folder will contain a file yago.n3
  The N3 version of YAGO does not contain reified facts.

* Database:
  The database schema is
        FACTS(id, relation, arg1, arg2)
  The FACTS table contains the facts of the ontology, each with a
  (numeric) id, the name of the relation, and the two arguments.
  
* SPOTLX Database:
  The database schema is
        RELATIONALFACTS(id, relation, arg1, arg2, timeBegin, timeEnd, location
         locationLatitude, locationLongitude, primaryWitness, context)
  The RELATIONALFACTS table contains the facts of the ontology, each with a
  (numeric) id, the name of the relation, and the two arguments.
  Additionally, timeBegin/timeEnd are set to the appropriate time values, 
  as well as locationLatitude/locationLongitude to the respective location.
  PrimaryWitness contains the source of the fact, and context contains
  tab-separated keyphrases associated with the fact (or fact entities)

* XML:
  The XML output produces 2 files:
   * an XML file and an DTD file for facts
   * an XML file and an DTD file for entities
  For facts, the XML file basically contains one block per relation.
  Where each block contains blocks for each fact. Each fact block
  contains the arguments.
  For entities, the XML file contains one block per entity.

* Link to DBpedia
  The output is a single file called "DBpediaYAGOlink.n3", which
  contains lines in the "Notation 3" of the form
      <http://dbpedia.org/class/yago/class-name>
           owl:equivalentClass
      <http://yago-knowledge.org/resource/class-name>

      <http://dbpedia.org/resource/individual-name>
         owl:sameAs
      <http://yago-knowledge.org/resource/individual-name>

* Export for DBpedia
  The output is a single file called "DBpediaYAGOexport.n3", which
  contains "Notation 3" lines of the following YAGO relations:
      means, subclassof, type

* Jena TDB
  The output is a folder that contains Jena implementation specific
  database files.
  
------------------ Contact -----------------------------------------

If you have any problems, questions or suggestions, please send a mail
to yagoZ@mpi-inf.mpg.de (remove the 'Z').

------------------ Appendix ----------------------------------------

The YAGO converters ship with a version of the Jena Semantic Web Framework.

Therefore, we reproduce the Jena license here:

(c) Copyright 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009 Hewlett-Packard Development Company, LP

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

   1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
   2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
   3. The name of the author may not be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 
Jena includes software developed by the Apache Software Foundation (http://www.apache.org/).

Jena includes RDF schemes from DCMI:

    Portions of this software may use RDF schemas Copyright (c) 2006 DCMI, the Dublin Core Metadata Initiative. These are licensed under the Creative Commons 3.0 Attribution license. 

Jena is built on top of other sub-systems which we gratefully acknowledge: details of these systems and their version numbers.

YourKit is kindly supporting open source projects with its full-featured Java Profiler. YourKit, LLC is the creator of innovative and intelligent tools for profiling Java and .NET applications. Take a look at YourKit's leading software products: YourKit Java Profiler and YourKit .NET Profiler.

The converters also use Berkeley Java DB, with the following license:
=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
/*
 * Copyright (c) 2002-2009 Oracle.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Redistributions in any form must be accompanied by information on
 *    how to obtain complete source code for the DB software and any
 *    accompanying software that uses the DB software.  The source code
 *    must either be included in the distribution or be available for no
 *    more than the cost of distribution plus a nominal fee, and must be
 *    freely redistributable under reasonable conditions.  For an
 *    executable file, complete source code means the source code for all
 *    modules it contains.  It does not include source code for modules or
 *    files that typically accompany the major components of the operating
 *    system on which the executable file runs.
 *
 * THIS SOFTWARE IS PROVIDED BY ORACLE ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR
 * NON-INFRINGEMENT, ARE DISCLAIMED.  IN NO EVENT SHALL ORACLE BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN
 * IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
/***
 * ASM: a very small and fast Java bytecode manipulation framework
 * Copyright (c) 2000-2005 INRIA, France Telecom
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holders nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

  