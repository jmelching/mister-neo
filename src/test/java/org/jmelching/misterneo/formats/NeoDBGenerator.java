package org.jmelching.misterneo.formats;

import org.junit.Before;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class NeoDBGenerator {

    enum MyRelationshipTypes implements RelationshipType {
        MARRIED_TO, CHILD_OF
    }

    @Before
    public static void main(String[] args) {

        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase(args[0]);
        Transaction tx = db.beginTx();

        Node jeff = db.createNode();
        Node jessica = db.createNode();
        Node max = db.createNode();
        jeff.setProperty("name", "jeff");
        jessica.setProperty("name", "jessica");
        jeff.createRelationshipTo(jessica, MyRelationshipTypes.MARRIED_TO);
        jessica.createRelationshipTo(jeff, MyRelationshipTypes.MARRIED_TO);

        jessica.createRelationshipTo(max, MyRelationshipTypes.CHILD_OF);

        tx.success();
        tx.finish();
        db.shutdown();

    }

}
