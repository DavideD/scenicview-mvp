/**
 * Hibernate ScenicView, Great Views on your Data
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.scenicview.neo4j.internal;

import org.hibernate.scenicview.spi.backend.DenormalizationBackend;
import org.hibernate.scenicview.spi.backend.DenormalizationBackendFactory;

/**
 * @author Gunnar Morling
 *
 */
public class Neo4jEmbeddedBackendFactory implements DenormalizationBackendFactory {

	@Override
	public String getName() {
		return "neo4j_embedded";
	}

	@Override
	public DenormalizationBackend createNewBackend(String connectionUri) {
		return new Neo4jEmbeddedDenormalizationBackend( connectionUri );
	}
}
