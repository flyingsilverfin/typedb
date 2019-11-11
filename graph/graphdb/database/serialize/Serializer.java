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

package grakn.core.graph.graphdb.database.serialize;

import grakn.core.graph.diskstorage.ScanBuffer;
import grakn.core.graph.graphdb.database.serialize.AttributeHandler;
import grakn.core.graph.graphdb.database.serialize.DataOutput;

import java.io.Closeable;

public interface Serializer extends AttributeHandler, Closeable {

    Object readClassAndObject(ScanBuffer buffer);

    <T> T readObject(ScanBuffer buffer, Class<T> type);

    <T> T readObjectByteOrder(ScanBuffer buffer, Class<T> type);

    <T> T readObjectNotNull(ScanBuffer buffer, Class<T> type);

    DataOutput getDataOutput(int initialCapacity);

}
