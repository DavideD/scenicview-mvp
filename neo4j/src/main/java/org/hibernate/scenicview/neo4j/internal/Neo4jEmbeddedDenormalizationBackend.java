/**
 * Hibernate ScenicView, Great Views on your Data
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.scenicview.neo4j.internal;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hibernate.scenicview.spi.backend.DenormalizationBackend;
import org.hibernate.scenicview.spi.backend.model.ColumnSequence;
import org.hibernate.scenicview.spi.backend.model.DeleteTask;
import org.hibernate.scenicview.spi.backend.model.DenormalizationTaskHandler;
import org.hibernate.scenicview.spi.backend.model.DenormalizationTaskSequence;
import org.hibernate.scenicview.spi.backend.model.TreeTraversalSequence;
import org.hibernate.scenicview.spi.backend.model.TreeTraversalSequence.TreeTraversalEvent;
import org.hibernate.scenicview.spi.backend.model.UpsertTask;
import org.hibernate.scenicview.spi.backend.type.TypeProvider;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * @author Gunnar Morling
 */
public class Neo4jEmbeddedDenormalizationBackend implements DenormalizationBackend {

	private static final Pattern CONNECTION = Pattern.compile( "(.*?):(.*?)" );

	private final GraphDatabaseService database;

	public Neo4jEmbeddedDenormalizationBackend(String connectionUri) {
		this.database = createConnection( connectionUri );
	}

	private static GraphDatabaseService createConnection(String connectionUri) {
		Matcher matcher = CONNECTION.matcher( connectionUri );
		if ( !matcher.matches() ) {
			throw new IllegalArgumentException( "Unexpected connection string: " + connectionUri );
		}

		String dbPath = matcher.group( 1 );
		File dbFile = new File( dbPath );
		GraphDatabaseService service = new GraphDatabaseFactory().newEmbeddedDatabase( dbFile );
		return service;
	}

	public GraphDatabaseService getDatabase() {
		return database;
	}

	@Override
	public TypeProvider getTypeProvider() {
		return new Neo4jEmbeddedTypeProvider();
	}

	@Override
	public void stop() {
		database.shutdown();
	}

	@Override
	public void process(DenormalizationTaskSequence tasks) {
		Transaction tx = database.beginTx();
		try {
			tasks.forEach( new NodeCreationAndInsertionHandler() );
			tx.success();
		}
		catch (Exception e) {
			tx.failure();
			throw new RuntimeException( e );
		}
		finally {
			tx.close();
		}
	}

	class NodeCreationAndInsertionHandler implements DenormalizationTaskHandler {

		@Override
		public void handleDelete(DeleteTask delete) {
			// TODO Auto-generated method stub
			DenormalizationTaskHandler.super.handleDelete( delete );
		}

		@Override
		public void handleUpsert(UpsertTask upsert) {
			Node root = getAsNode( upsert.getCollectionName(), upsert.getObjectTree() );
			addId( root, upsert.getPrimaryKey() );
		}

		private Node getAsNode(String collectionName, TreeTraversalSequence sequence) {
			Deque<Object> hierarchy = new ArrayDeque<>();

			sequence.forEach(
					null,
					( event, context ) -> {
						switch( event.getType() ) {
							case AGGREGATE_ROOT_START:
								Node rootNode = updateRootNode( collectionName, hierarchy, event );
								hierarchy.push( rootNode );
								break;
							case AGGREGATE_ROOT_END:
								break;
							case OBJECT_START:
								Node start = (Node) hierarchy.peek();
								Node associatedNode = updateAssociatedObject( start, event );
								hierarchy.push( associatedNode );
								break;
							case OBJECT_END:
								Node node = (Node) hierarchy.pop();
								// TODO: It's actually more complicated than this. With a query it would be easier.
								if ( !node.getPropertyKeys().iterator().hasNext() ) {
									Iterator<Relationship> outgoing = node.getRelationships( Direction.OUTGOING ).iterator();
									if ( !outgoing.hasNext() ) {
										// No outgoing relationships
										Iterator<Relationship> incoming = node.getRelationships( Direction.INCOMING ).iterator();
										Relationship rel = null;
										if ( incoming.hasNext() ) {
											rel = incoming.next();
										}
										if ( !incoming.hasNext() ) {
											// Only one incoming relationship
											// We can delete the relationship and the node
											rel.delete();
											node.delete();
										}
									}
								}
								break;
							case COLLECTION_START:
								hierarchy.push( hierarchy.peek() );
								break;
							case COLLECTION_END:
								hierarchy.pop();
								break;
						}
			} );

			return (Node) hierarchy.pop();
		}

		private Node updateRootNode(String collectionName, Deque<Object> hierarchy, TreeTraversalEvent event) {
			Label label = DynamicLabel.label( collectionName );
			Node rootNode = toNode( event.getColumnSequence(), label );
			return rootNode;
		}

		private Node updateAssociatedObject(Node start, TreeTraversalEvent event) {
			DynamicRelationshipType relationshipType = DynamicRelationshipType.withName( event.getName() );
			//TODO: Destination label
			Node end = toNode( event.getColumnSequence() );
			start.createRelationshipTo( end, relationshipType );
			return end;
		}

		private Node toNode(ColumnSequence properties, Label... labels) {
			final Node node = database.createNode( labels );
			if ( properties != null ) {
				properties.forEach( (name, value) -> {
					if ( value != null ) {
						node.setProperty( name, value );
					}
				} );
			}
			return node;
		}

		private void addId(Node node, ColumnSequence primaryKey) {
			primaryKey.forEach( (name, value) -> {
				node.setProperty( name, value );
			});
		}
	}
}
