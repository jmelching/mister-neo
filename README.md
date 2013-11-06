mister-neo
==========

A set of hadoop map-reduce (mister) tools and input formats for working with raw neo4j data files. 

These tools are very useful when working with a very large neo4j database and you need to extract an adjacency or edge list file format for use in other tools like graphlab, graphchi, faunus, etc.

Tool list:

All of these tools can run on hadoop or local.

org.jmelching.AdjacencyListRunner  
command line args: neostore.relationshipstore.db outputdir

This produces an adjacency file as explained here: http://docs.graphlab.org/graph_formats.html



