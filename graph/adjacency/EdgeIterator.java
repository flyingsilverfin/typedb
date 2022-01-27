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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;

public abstract class EdgeIterator {

    public static abstract class Thing<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>> {

        final ThingVertex owner;
        final Seekable<EDGE_VIEW, Order.Asc> edges;

        private Thing(ThingVertex owner, Seekable<EDGE_VIEW, Order.Asc> edges) {
            this.owner = owner;
            this.edges = edges;
        }

        public static class In extends Thing<ThingEdge.View.Backward> {

            final Encoding.Edge.Thing encoding;

            public In(ThingVertex owner, Seekable<ThingEdge.View.Backward, Order.Asc> edges, Encoding.Edge.Thing encoding) {
                super(owner, edges);
                this.encoding = encoding;
            }

            public Seekable<ThingVertex, Order.Asc> from() {
                return edges.mapSorted(ASC, ThingEdge.View.Backward::from, this::targetEdge);
            }

            public SortedIterator<ThingVertex, Order.Asc> to() {
                return iterateSorted(ASC, list(owner));
            }

            ThingEdge.View.Backward targetEdge(ThingVertex targetFrom) {
                return new ThingEdgeImpl.Target(encoding, targetFrom, owner, null).getBackward();
            }

            public static class Optimised extends In {

                private final TypeVertex optimisedType;

                public Optimised(ThingVertex owner,
                                 Seekable<ThingEdge.View.Backward, Order.Asc> edges,
                                 Encoding.Edge.Thing encoding,
                                 TypeVertex optimisedType) {
                    super(owner, edges, encoding);
                    this.optimisedType = optimisedType;
                }

                public Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> fromAndOptimised() {
                    return edges.mapSorted(
                            ASC,
                            edgeView -> KeyValue.of(edgeView.from(), edgeView.optimised().get()),
                            fromAndOptimised -> targetEdge(fromAndOptimised.key())
                    );
                }

                @Override
                ThingEdge.View.Backward targetEdge(ThingVertex targetFrom) {
                    return new ThingEdgeImpl.Target(encoding, targetFrom, owner, optimisedType).getBackward();
                }
            }
        }

        public static class Out extends EdgeIterator.Thing<ThingEdge.View.Forward> {

            final Encoding.Edge.Thing encoding;

            public Out(ThingVertex owner, Seekable<ThingEdge.View.Forward, Order.Asc> edges, Encoding.Edge.Thing encoding) {
                super(owner, edges);
                this.encoding = encoding;
            }

            ThingEdge.View.Forward targetEdge(ThingVertex targetTo) {
                return new ThingEdgeImpl.Target(encoding, owner, targetTo, null).getForward();
            }

            public SortedIterator<ThingVertex, Order.Asc> from() {
                return iterateSorted(ASC, list(owner));
            }

            public Seekable<ThingVertex, Order.Asc> to() {
                return edges.mapSorted(ASC, ThingEdge.View.Forward::to, this::targetEdge);
            }

            public static class Optimised extends Out {

                private final TypeVertex optimisedType;

                public Optimised(ThingVertex owner,
                                 Seekable<ThingEdge.View.Forward, Order.Asc> edges,
                                 Encoding.Edge.Thing encoding,
                                 TypeVertex optimisedType) {
                    super(owner, edges, encoding);
                    this.optimisedType = optimisedType;
                }

                public Seekable<KeyValue<ThingVertex, ThingVertex>, Order.Asc> toAndOptimised() {
                    return edges.mapSorted(
                            ASC,
                            edgeView -> KeyValue.of(edgeView.to(), edgeView.optimised().get()),
                            toAndOptimised -> targetEdge(toAndOptimised.key())
                    );
                }

                @Override
                ThingEdge.View.Forward targetEdge(ThingVertex targetTo) {
                    return new ThingEdgeImpl.Target(encoding, owner, targetTo, optimisedType).getForward();
                }
            }
        }
    }
}
