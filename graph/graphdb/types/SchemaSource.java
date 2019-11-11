/*
 * GRAKN.AI - THE KNOWLEDGE GRAPH
 * Copyright (C) 2019 Grakn Labs Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package grakn.core.graph.graphdb.types;

import com.google.common.base.Preconditions;
import grakn.core.graph.core.schema.SchemaStatus;
import org.apache.tinkerpop.gremlin.structure.Direction;


public interface SchemaSource {

    long longId();

    String name();

    SchemaStatus getStatus();

    TypeDefinitionMap getDefinition();

    Iterable<Entry> getRelated(TypeDefinitionCategory def, Direction dir);

    /**
     * Resets the internal caches used to speed up lookups on this schema source.
     * This is needed when the source gets modified.
     */
    void resetCache();

    IndexType asIndexType();

    class Entry {

        private final SchemaSource schemaType;
        private final Object modifier;

        public Entry(SchemaSource schemaType, Object modifier) {
            this.schemaType = Preconditions.checkNotNull(schemaType);
            this.modifier = modifier;
        }

        public SchemaSource getSchemaType() {
            return schemaType;
        }

        public Object getModifier() {
            return modifier;
        }
    }

}
