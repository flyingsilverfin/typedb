/*
 * Copyright (C) 2021 Vaticle
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
 *
 */

package com.vaticle.typedb.core.graph.adjacency;

import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.graph.edge.Edge;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.vertex.Vertex;

import java.util.List;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;

public abstract class EdgeIterator<
        EDGE extends Edge<?, EDGE_IID, VERTEX>,
        EDGE_IID extends EdgeIID<?, ?, ?, ?>,
        VERTEX extends Vertex<?, ?>> {

    final VERTEX owner;
    final Seekable<ComparableEdge<EDGE, EDGE_IID>, Order.Asc> edges;

    EdgeIterator(VERTEX owner, Seekable<ComparableEdge<EDGE, EDGE_IID>, Order.Asc> edges) {
        this.owner = owner;
        this.edges = edges;
    }

    abstract static class In<
            EDGE extends Edge<?, EDGE_IID, VERTEX>,
            EDGE_IID extends EdgeIID<?, ?, ?, ?>,
            VERTEX extends Vertex<?, ?>> extends EdgeIterator<EDGE, EDGE_IID, VERTEX> {

        In(VERTEX owner, Seekable<ComparableEdge<EDGE, EDGE_IID>, Order.Asc> edges) {
            super(owner, edges);
        }

        abstract EDGE targetEdge(VERTEX targetFrom);

        public Seekable<VERTEX, Order.Asc> from() {
            return edges.mapSorted(
                    ASC,
                    comparableEdge -> comparableEdge.edge().from(),
                    vertex -> ComparableEdge.byInIID(targetEdge(vertex))
            );
        }

        public SortedIterator<VERTEX, Order.Asc> to() {
            List<VERTEX> list = list(owner);
            return iterateSorted(ASC, list);
        }
    }

    abstract static class Out<
            EDGE extends Edge<?, EDGE_IID, VERTEX>,
            EDGE_IID extends EdgeIID<?, ?, ?, ?>,
            VERTEX extends Vertex<?, ?>> extends EdgeIterator<EDGE, EDGE_IID, VERTEX> {

        Out(VERTEX owner, Seekable<ComparableEdge<EDGE, EDGE_IID>, Order.Asc> edges) {
            super(owner, edges);
        }

        abstract EDGE targetEdge(VERTEX targetTo);

        SortedIterator<VERTEX, Order.Asc> from() {
            return iterateSorted(ASC, list(owner));
        }

        Seekable<VERTEX, Order.Asc> to() {
            return edges.mapSorted(
                    ASC,
                    comparableEdge -> comparableEdge.edge().to(),
                    vertex -> ComparableEdge.byOutIID(targetEdge(vertex))
            );
        }
    }

    public static class ComparableEdge<EDGE extends Edge<?, EDGE_IID, ?>, EDGE_IID extends EdgeIID<?, ?, ?, ?>>
            implements Comparable<ComparableEdge<EDGE, EDGE_IID>> {

        private final EDGE edge;
        private final EDGE_IID comparableIID;

        ComparableEdge(EDGE edge, EDGE_IID comparableIID) {
            this.edge = edge;
            this.comparableIID = comparableIID;
        }

        static <EDGE extends Edge<?, EDGE_IID, ?>, EDGE_IID extends EdgeIID<?, ?, ?, ?>> ComparableEdge<EDGE, EDGE_IID>
        byInIID(EDGE edge) {
            return new ComparableEdge<>(edge, edge.inIID());
        }

        static <EDGE extends Edge<?, EDGE_IID, ?>, EDGE_IID extends EdgeIID<?, ?, ?, ?>> ComparableEdge<EDGE, EDGE_IID>
        byOutIID(EDGE edge) {
            return new ComparableEdge<>(edge, edge.outIID());
        }

        public EDGE edge() {
            return edge;
        }

        public EDGE_IID iid() {
            return comparableIID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final ComparableEdge that = (ComparableEdge) o;
            return iid().equals(that.iid());
        }

        @Override
        public int hashCode() {
            return iid().hashCode();
        }

        @Override
        public int compareTo(ComparableEdge<EDGE, EDGE_IID> other) {
            return iid().compareTo(other.iid());
        }
    }
}
