/**
 * Hibernate ScenicView, Great Views on your Data
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.scenicview.neo4j.test;

import static org.fest.assertions.Assertions.assertThat;
import static org.hibernate.scenicview.neo4j.test.dsl.GraphAssertions.assertThatExists;
import static org.hibernate.scenicview.neo4j.test.dsl.GraphAssertions.node;

import java.util.Arrays;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.scenicview.config.DenormalizationJobConfigurator;
import org.hibernate.scenicview.internal.backend.BackendManager;
import org.hibernate.scenicview.neo4j.internal.Neo4jEmbeddedDenormalizationBackend;
import org.hibernate.scenicview.neo4j.test.dsl.NodeForGraphAssertions;
import org.hibernate.scenicview.test.poc.model.Actor;
import org.hibernate.scenicview.test.poc.model.Genre;
import org.hibernate.scenicview.test.poc.model.Location;
import org.hibernate.scenicview.test.poc.model.Money;
import org.hibernate.scenicview.test.poc.model.Movie;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.tooling.GlobalGraphOperations;

public class DenormalizationPoCIT {

	private static final String CONNECTION_ID = "some-neo4j";

	public static class MyDenormalizationJobConfig implements DenormalizationJobConfigurator {

		@Override
		public void configure(Builder builder) {
			builder.newDenormalizationJob( "ActorsWithMoviesAndGenreAndRating" )
					.withAggregateRoot( Actor.class )
					.withAssociation( Actor::getFavoriteGenre )
					.withCollection( Actor::getPlayedIn )
					.includingCollection( Movie::getFilmedAt )
					.withCollection( Actor::getRatings )
					.usingCollectionName( "ActorWithDependencies" )
					.usingConnectionId( CONNECTION_ID )
					.build();
		}
	}

	private EntityManagerFactory entityManagerFactory;

	@Before
	public void init() {
		entityManagerFactory = Persistence.createEntityManagerFactory( "scenicViewTestPu" );
	}

	@After
	public void destroy() {
		if ( entityManagerFactory != null ) {
			entityManagerFactory.close();
		}
	}

	@Test
	public void canDenormalize() throws Exception {
		EntityManager entityManager = entityManagerFactory.createEntityManager();
		entityManager.getTransaction().begin();

		Location location1 = new Location( "Orlando", 27.8 );
		entityManager.persist( location1 );

		Location location2 = new Location( "Helsinki", 8.9 );
		entityManager.persist( location2 );

		Movie movie1 = new Movie();
		movie1.setName( "It happened in the winter" );
		movie1.setYearOfRelease( 1980 );
		movie1.getFilmedAt().add( location1 );
		movie1.getFilmedAt().add( location2 );
		entityManager.persist( movie1 );

		Movie movie2 = new Movie();
		movie2.setName( "If you knew" );
		movie2.setYearOfRelease( 1965 );
		entityManager.persist( movie2 );

		Genre thriller = new Genre();
		thriller.setName( "Thriller" );
		thriller.setSuitedForChildren( false );
		entityManager.persist( thriller );

		Actor frank = new Actor();
		frank.setName( "Franky" );
		frank.setFavoriteGenre( thriller );
		frank.getPlayedIn().add( movie1 );
		frank.getPlayedIn().add( movie2 );
		frank.setSalary( new Money( 98, "¥" ) );
		frank.getRatings().addAll( Arrays.asList( 9, 8, 5, 4 ) );
		entityManager.persist( frank );

		entityManager.getTransaction().commit();
		entityManager.close();

		NodeForGraphAssertions frankNode = node( "actor" )
				.property( "id", frank.getId() )
				.property( "name", frank.getName() )
				.property( "salary_currency", frank.getSalary().getCurrency() )
				.property( "salary_amount", frank.getSalary().getAmount() )
				;

		assertThatOnlyTheseNodesExist( frankNode );
	}

	protected void assertThatOnlyTheseNodesExist(NodeForGraphAssertions... nodes) throws Exception {
		GraphDatabaseService database = createExecutionEngine();
		for ( NodeForGraphAssertions node : nodes ) {
			assertThatExists( database, node );
		}
		ResourceIterable<Node> allNodes = GlobalGraphOperations.at( database ).getAllNodes();
		int size = 0;
		for ( @SuppressWarnings("unused") Node node : allNodes ) {
			size++;
		}
		assertThat( size ).equals( nodes.length );
	}

	private org.neo4j.graphdb.GraphDatabaseService createExecutionEngine() {
		Neo4jEmbeddedDenormalizationBackend neo4jBackend = (Neo4jEmbeddedDenormalizationBackend) entityManagerFactory.unwrap( SessionFactoryImplementor.class )
				.getServiceRegistry()
				.getService( BackendManager.class )
				.getBackend( CONNECTION_ID );

		return neo4jBackend.getDatabase();
}
}
