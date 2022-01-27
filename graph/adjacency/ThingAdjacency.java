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

import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.Edge;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.IID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;

public interface ThingAdjacency {

    interface In extends ThingAdjacency {

        InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookAhead);

        OptimisedInEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookAhead);

        @Override
        default boolean isIn() {
            return true;
        }

        class InEdgeIterator extends EdgeIterator.In<ThingEdge, EdgeIID.Thing, ThingVertex> {

            final Encoding.Edge.Thing encoding;

            public InEdgeIterator(ThingVertex owner,
                                  Seekable<ComparableEdge<ThingEdge, EdgeIID.Thing>, Order.Asc> edges,
                                  Encoding.Edge.Thing encoding) {
                super(owner, edges);
                this.encoding = encoding;
            }

            @Override
            ThingEdge targetEdge(ThingVertex targetFrom) {
                return new ThingEdgeImpl.Target(encoding, targetFrom, owner, null);
            }

        }

        class OptimisedInEdgeIterator extends InEdgeIterator {

            private final TypeVertex optimisedType;

            public OptimisedInEdgeIterator(ThingVertex owner,
                                           Seekable<ComparableEdge<ThingEdge, EdgeIID.Thing>, Order.Asc> edges,
                                           Encoding.Edge.Thing encoding,
                                           TypeVertex optimisedType) {
                super(owner, edges, encoding);
                this.optimisedType = optimisedType;
            }

            Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> fromAndOptimised() {
                return edges.mapSorted(
                        ASC,
                        comparableEdge -> KeyValue.of(comparableEdge.edge().from(), comparableEdge.edge().optimised().get()),
                        fromAndOptimised -> ComparableEdge.byInIID(targetEdge(fromAndOptimised.key()))
                );
            }

            @Override
            ThingEdge targetEdge(ThingVertex targetFrom) {
                return new ThingEdgeImpl.Target(encoding, targetFrom, owner, optimisedType);
            }
        }
    }

    interface Out extends ThingAdjacency {

        OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookAhead);

        OptimisedOutEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookAhead);

        @Override
        default boolean isOut() {
            return true;
        }

        class OutEdgeIterator extends EdgeIterator.Out<ThingEdge, EdgeIID.Thing, ThingVertex> {

            final Encoding.Edge.Thing encoding;

            public OutEdgeIterator(ThingVertex owner,
                                   Seekable<ComparableEdge<ThingEdge, EdgeIID.Thing>, Order.Asc> edges,
                                   Encoding.Edge.Thing encoding) {
                super(owner, edges);
                this.encoding = encoding;
            }

            @Override
            ThingEdge targetEdge(ThingVertex targetTo) {
                return new ThingEdgeImpl.Target(encoding, owner, targetTo, null);
            }

        }

        class OptimisedOutEdgeIterator extends OutEdgeIterator {

            private final TypeVertex optimisedType;

            public OptimisedOutEdgeIterator(ThingVertex owner,
                                            Seekable<ComparableEdge<ThingEdge, EdgeIID.Thing>, Order.Asc> edges,
                                            Encoding.Edge.Thing encoding,
                                            TypeVertex optimisedType) {
                super(owner, edges, encoding);
                this.optimisedType = optimisedType;
            }

            Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> fromAndOptimised() {
                return edges.mapSorted(
                        ASC,
                        comparableEdge -> KeyValue.of(comparableEdge.edge().to(), comparableEdge.edge().optimised().get()),
                        toAndOptimised -> ComparableEdge.byOutIID(targetEdge(toAndOptimised.key()))
                );
            }

            @Override
            ThingEdge targetEdge(ThingVertex targetTo) {
                return new ThingEdgeImpl.Target(encoding, owner, targetTo, optimisedType);
            }
        }
    }

    UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding);

    /**
     * Returns an edge of type {@code encoding} that connects to an {@code adjacent}
     * vertex.
     *
     * @param encoding type of the edge to filter by
     * @param adjacent vertex that the edge connects to
     * @return an edge of type {@code encoding} that connects to {@code adjacent}.
     */
    ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent);

    /**
     * Returns an edge of type {@code encoding} that connects to an {@code adjacent}
     * vertex, that is an optimisation edge over a given {@code optimised} vertex.
     *
     * @param encoding  type of the edge to filter by
     * @param adjacent  vertex that the edge connects to
     * @param optimised vertex that this optimised edge is compressing
     * @return an edge of type {@code encoding} that connects to {@code adjacent} through {@code optimised}.
     */
    ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised);

    default boolean isIn() {
        return false;
    }

    default boolean isOut() {
        return false;
    }

    class UnsortedEdgeIterator {

        private final FunctionalIterator<ThingEdge> edgeIterator;

        public UnsortedEdgeIterator(FunctionalIterator<ThingEdge> edgeIterator) {
            this.edgeIterator = edgeIterator;
        }

        public FunctionalIterator<ThingVertex> from() {
            return edgeIterator.map(Edge::from);
        }

        public FunctionalIterator<ThingVertex> to() {
            return edgeIterator.map(Edge::to);
        }
    }

    interface Write extends ThingAdjacency {

        interface In extends Write, ThingAdjacency.In {

        }

        interface Out extends Write, ThingAdjacency.Out {

        }

        /**
         * Puts an adjacent vertex over an edge with a given encoding.
         *
         * The owner of this {@code Adjacency} map will also be added as an adjacent
         * vertex to the provided vertex, through an opposite facing edge stored in
         * an {@code Adjacency} map with an opposite direction to this one. I.e.
         * This is a recursive put operation.
         *
         * @param encoding   of the edge that will connect the owner to the adjacent vertex
         * @param adjacent   the adjacent vertex
         * @param isInferred
         * @return an edge of type {@code encoding} that connects to {@code adjacent}.
         */
        ThingEdge put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, boolean isInferred);

        /**
         * Puts an edge of type {@code encoding} from the owner to an adjacent vertex,
         * which is an optimisation edge over a given {@code optimised} vertex.
         *
         * The owner of this {@code Adjacency} map will also be added as an adjacent
         * vertex to the provided vertex, through an opposite facing edge stored in
         * an {@code Adjacency} map with an opposite direction to this one. I.e.
         * This is a recursive put operation.
         *
         * @param encoding   type of the edge
         * @param adjacent   the adjacent vertex
         * @param optimised  vertex that this optimised edge is compressing
         * @param isInferred
         * @return an edge of type {@code encoding} that connects to {@code adjacent}.
         */
        ThingEdge put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, ThingVertex.Write optimised, boolean isInferred);

        /**
         * Deletes all edges with a given encoding from the {@code Adjacency} map.
         *
         * This is a recursive delete operation. Deleting the edges from this
         * {@code Adjacency} map will also delete it from the {@code Adjacency} map
         * of the previously adjacent vertex.
         *
         * @param encoding type of the edge to the adjacent vertex
         */
        void delete(Encoding.Edge.Thing encoding);

        /**
         * Deletes a set of edges that match the provided properties.
         *
         * @param encoding  type of the edge to filter by
         * @param lookAhead information of the adjacent edge to filter the edges with
         */
        void delete(Encoding.Edge.Thing encoding, IID... lookAhead);

        void deleteAll();

        ThingEdge cache(ThingEdge edge);

        void remove(ThingEdge edge);

        void commit();

    }

}
