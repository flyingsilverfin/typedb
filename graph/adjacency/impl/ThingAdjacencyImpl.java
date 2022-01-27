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

package com.vaticle.typedb.core.graph.adjacency.impl;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.core.common.collection.ByteArray;
import com.vaticle.typedb.core.common.collection.KeyValue;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.graph.adjacency.EdgeIterator.ComparableEdge;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Storage.Key;
import com.vaticle.typedb.core.graph.edge.Edge;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.iid.EdgeIID;
import com.vaticle.typedb.core.graph.iid.IID;
import com.vaticle.typedb.core.graph.iid.InfixIID;
import com.vaticle.typedb.core.graph.iid.SuffixIID;
import com.vaticle.typedb.core.graph.vertex.ThingVertex;
import com.vaticle.typedb.core.graph.vertex.TypeVertex;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.emptySorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.Sorted.iterateSorted;
import static com.vaticle.typedb.core.common.iterator.Iterators.iterate;
import static com.vaticle.typedb.core.common.iterator.Iterators.link;
import static com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.ASC;
import static com.vaticle.typedb.core.graph.common.Encoding.Edge.Thing.Optimised.ROLEPLAYER;
import static java.util.Arrays.copyOfRange;

public abstract class ThingAdjacencyImpl implements ThingAdjacency {

    InfixIID.Thing directedInfixIID(Encoding.Edge.Thing encoding, IID... lookAhead) {
        return isOut() ? InfixIID.Thing.of(encoding.out(), lookAhead) : InfixIID.Thing.of(encoding.in(), lookAhead);
    }

    ComparableEdge<ThingEdge, EdgeIID.Thing> toComparable(ThingEdge edge) {
        return isOut() ? ComparableEdge.byOutIID(edge) : ComparableEdge.byInIID(edge);
    }

    public static abstract class Read extends ThingAdjacencyImpl {

        final ThingVertex owner;

        Read(ThingVertex owner) {
            this.owner = owner;
        }

        SortedIterator.Seekable<ComparableEdge<ThingEdge, EdgeIID.Thing>, Order.Asc> edgeIteratorSorted(
                Encoding.Edge.Thing encoding, IID... lookahead) {
            Key.Prefix<EdgeIID.Thing> prefix = EdgeIID.Thing.prefix(owner.iid(), directedInfixIID(encoding, lookahead));
            return owner.graph().storage().iterate(prefix, ASC).mapSorted(
                    ASC,
                    kv -> toComparable(newPersistedEdge(EdgeIID.Thing.of(kv.key().bytes()))),
                    sortable -> KeyValue.of(sortable.iid(), ByteArray.empty())
            );
        }

        private ThingEdgeImpl.Persisted newPersistedEdge(EdgeIID.Thing of) {
            return new ThingEdgeImpl.Persisted(owner.graph(), of);
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
            EdgeIID.Thing edgeIID = EdgeIID.Thing.of(owner.iid(), directedInfixIID(encoding), adjacent.iid());
            if (owner.graph().storage().get(edgeIID) == null) return null;
            else return newPersistedEdge(edgeIID);
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            EdgeIID.Thing edgeIID = EdgeIID.Thing.of(
                    owner.iid(), directedInfixIID(encoding, optimised.iid().type()),
                    adjacent.iid(), SuffixIID.of(optimised.iid().key())
            );
            if (owner.graph().storage().get(edgeIID) == null) return null;
            else return newPersistedEdge(edgeIID);
        }

        @Override
        public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
            Key.Prefix<EdgeIID.Thing> prefix = EdgeIID.Thing.prefix(owner.iid(), directedInfixIID(encoding));
            return new UnsortedEdgeIterator(owner.graph().storage().iterate(prefix, ASC)
                    .map(kv -> newPersistedEdge(EdgeIID.Thing.of(kv.key().bytes()))));
        }

        public static class In extends Read implements ThingAdjacency.In {

            public In(ThingVertex owner) {
                super(owner);
            }

            @Override
            public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookAhead) {
                return new InEdgeIterator(owner, edgeIteratorSorted(encoding, lookAhead), encoding);
            }

            @Override
            public OptimisedInEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookAhead) {
                IID[] mergedLookahead = new IID[1 + lookAhead.length];
                mergedLookahead[0] = roleType.iid();
                System.arraycopy(lookAhead, 0, mergedLookahead, 1, lookAhead.length);
                return new OptimisedInEdgeIterator(owner, edgeIteratorSorted(encoding, mergedLookahead), encoding, roleType);
            }
        }

        public static class Out extends Read implements ThingAdjacency.Out {

            public Out(ThingVertex owner) {
                super(owner);
            }

            @Override
            public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookAhead) {
                return new OutEdgeIterator(owner, edgeIteratorSorted(encoding, lookAhead), encoding);
            }

            @Override
            public OptimisedOutEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookAhead) {
                IID[] mergedLookahead = new IID[1 + lookAhead.length];
                mergedLookahead[0] = roleType.iid();
                System.arraycopy(lookAhead, 0, mergedLookahead, 1, lookAhead.length);
                return new OptimisedOutEdgeIterator(owner, edgeIteratorSorted(encoding, mergedLookahead), encoding, roleType);
            }
        }
    }

    public static abstract class Write extends ThingAdjacencyImpl implements ThingAdjacency.Write {

        final ThingVertex.Write owner;
        final ConcurrentMap<InfixIID.Thing, ConcurrentSet<InfixIID.Thing>> infixes;
        final ConcurrentMap<InfixIID.Thing, ConcurrentNavigableMap<ComparableEdge.Thing, ThingEdge>> edges;

        Write(ThingVertex.Write owner) {
            this.owner = owner;
            this.infixes = new ConcurrentHashMap<>();
            this.edges = new ConcurrentHashMap<>();
        }

        IID[] infixTails(ThingEdge edge) {
            if (edge.encoding().isOptimisation()) {
                if (isOut()) {
                    return new IID[]{edge.outIID().infix().asRolePlayer().tail(), edge.toIID().prefix(), edge.toIID().type()};
                } else {
                    return new IID[]{edge.inIID().infix().asRolePlayer().tail(), edge.fromIID().prefix(), edge.fromIID().type()};
                }
            } else {
                if (isOut()) return new IID[]{edge.toIID().prefix(), edge.toIID().type()};
                else return new IID[]{edge.fromIID().prefix(), edge.fromIID().type()};
            }
        }

        SortedIterator.Seekable<ComparableEdge.Thing, Order.Asc> bufferedEdgeIterator(Encoding.Edge.Thing encoding, IID[] lookahead) {
            ConcurrentNavigableMap<ComparableEdge.Thing, ThingEdge> result;
            InfixIID.Thing infixIID = directedInfixIID(encoding, lookahead);
            if (lookahead.length == encoding.lookAhead()) {
                return (result = edges.get(infixIID)) != null ? iterateSorted(ASC, result.keySet()) : emptySorted();
            }

            assert lookahead.length < encoding.lookAhead();
            Set<InfixIID.Thing> iids = new HashSet<>();
            iids.add(infixIID);
            for (int i = lookahead.length; i < encoding.lookAhead() && !iids.isEmpty(); i++) {
                Set<InfixIID.Thing> newIIDs = new HashSet<>();
                for (InfixIID.Thing iid : iids) {
                    Set<InfixIID.Thing> someNewIIDs = infixes.get(iid);
                    if (someNewIIDs != null) newIIDs.addAll(someNewIIDs);
                }
                iids = newIIDs;
            }

            return iterate(iids).mergeMap(ASC, iid -> {
                ConcurrentNavigableMap<ComparableEdge.Thing, ThingEdge> res;
                return (res = edges.get(iid)) != null ? iterateSorted(ASC, res.keySet()) : emptySorted();
            });
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            assert encoding.isOptimisation();
            Predicate<ThingEdge> predicate = isOut()
                    ? e -> e.to().equals(adjacent) && e.outIID().suffix().equals(SuffixIID.of(optimised.iid().key()))
                    : e -> e.from().equals(adjacent) && e.inIID().suffix().equals(SuffixIID.of(optimised.iid().key()));
            FunctionalIterator<ComparableEdge.Thing> iterator = bufferedEdgeIterator(
                    encoding, new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()}
            );
            // TODO optimise with seek()
            ThingEdge edge = null;
            while (iterator.hasNext()) {
                if (predicate.test(edge = iterator.next().edge())) break;
                else edge = null;
            }
            iterator.recycle();
            return edge;
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
            assert !encoding.isOptimisation();
            Predicate<ThingEdge> predicate = isOut() ? e -> e.to().equals(adjacent) : e -> e.from().equals(adjacent);
            FunctionalIterator<ComparableEdge.Thing> iterator = bufferedEdgeIterator(
                    encoding, new IID[]{adjacent.iid().prefix(), adjacent.iid().type()}
            );
            ThingEdge edge = null;
            // TODO optimise with seek()
            while (iterator.hasNext()) {
                if (predicate.test(edge = iterator.next().edge())) break;
                else edge = null;
            }
            iterator.recycle();
            return edge;
        }

        private ThingEdgeImpl put(Encoding.Edge.Thing encoding, ThingEdgeImpl edge, IID[] infixes,
                                  boolean isModified, boolean isReflexive) {
            assert encoding.lookAhead() == infixes.length;
            InfixIID.Thing infixIID = directedInfixIID(encoding);
            for (int i = 0; i < encoding.lookAhead(); i++) {
                this.infixes.computeIfAbsent(infixIID, x -> new ConcurrentSet<>()).add(
                        infixIID = directedInfixIID(encoding, copyOfRange(infixes, 0, i + 1))
                );
            }

            edges.compute(infixIID, (iid, cachedEdges) -> {
                if (cachedEdges == null) cachedEdges = new ConcurrentSkipListMap<>();
                ComparableEdge.Thing comparableEdge = toComparable(edge);
                ThingEdge thingEdge = cachedEdges.get(comparableEdge);
                if (thingEdge != null) {
                    if (thingEdge.isInferred() && !edge.isInferred()) thingEdge.isInferred(false);
                } else {
                    cachedEdges.put(comparableEdge, comparableEdge.edge());
                }
                return cachedEdges;
            });

            if (isModified) {
                assert !owner.isDeleted();
                owner.setModified();
            }
            if (isReflexive) {
                if (isOut()) ((ThingAdjacencyImpl.Write) edge.to().ins()).putNonReflexive(edge);
                else ((ThingAdjacencyImpl.Write) edge.from().outs()).putNonReflexive(edge);
            }
            return edge;
        }

        private void putNonReflexive(ThingEdgeImpl edge) {
            put(edge.encoding(), edge, infixTails(edge), true, false);
        }

        @Override
        public ThingEdgeImpl put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, boolean isInferred) {
            assert !encoding.isOptimisation();
            if (encoding == Encoding.Edge.Thing.Base.HAS && isOut() && !isInferred) {
                owner.graph().stats().hasEdgeCreated(owner.iid(), adjacent.iid().asAttribute());
            }
            ThingEdgeImpl edge = isOut()
                    ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent, isInferred)
                    : new ThingEdgeImpl.Buffered(encoding, adjacent, owner, isInferred);
            IID[] infixes = new IID[]{adjacent.iid().prefix(), adjacent.iid().type()};
            return put(encoding, edge, infixes, true, true);
        }

        @Override
        public ThingEdge put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, ThingVertex.Write optimised, boolean isInferred) {
            assert encoding.isOptimisation();
            ThingEdgeImpl edge = isOut()
                    ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent, optimised, isInferred)
                    : new ThingEdgeImpl.Buffered(encoding, adjacent, owner, optimised, isInferred);
            IID[] infixes = new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()};
            return put(encoding, edge, infixes, true, true);
        }

        @Override
        public ThingEdge cache(ThingEdge edge) {
            return put(edge.encoding(), (ThingEdgeImpl) edge, infixTails(edge), false, false);
        }

        @Override
        public void remove(ThingEdge edge) {
            InfixIID.Thing infixIID = directedInfixIID(edge.encoding(), infixTails(edge));
            if (edges.containsKey(infixIID)) {
                edges.get(infixIID).remove(toComparable(edge));
                owner.setModified();
            }
        }

        @Override
        public void deleteAll() {
            iterate(Encoding.Edge.Thing.Base.values()).forEachRemaining(this::delete);
            iterate(Encoding.Edge.Thing.Optimised.values()).forEachRemaining(this::delete);
        }

        @Override
        public void commit() {
            iterate(edges.values()).flatMap(edgeMap -> iterate(edgeMap.values()))
                    .filter(e -> !e.isInferred()).forEachRemaining(Edge::commit);
        }

        public static abstract class Buffered extends ThingAdjacencyImpl.Write {

            Buffered(ThingVertex.Write owner) {
                super(owner);
            }

            @Override
            public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
                return new UnsortedEdgeIterator(bufferedEdgeIterator(encoding, new IID[]{}).map(ComparableEdge.Thing::edge));
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                bufferedEdgeIterator(encoding, new IID[0]).forEachRemaining(comparableEdge -> comparableEdge.edge().delete());
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                bufferedEdgeIterator(encoding, lookAhead).forEachRemaining(comparableEdge -> comparableEdge.edge().delete());
            }

            public static class In extends Buffered implements com.vaticle.typedb.core.graph.adjacency.ThingAdjacency.Write.In {

                public In(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new InEdgeIterator(owner, bufferedEdgeIterator(encoding, lookahead), encoding);
                }

                @Override
                public OptimisedInEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookahead) {
                    IID[] mergedLookahead = new IID[1 + lookahead.length];
                    mergedLookahead[0] = roleType.iid();
                    System.arraycopy(lookahead, 0, mergedLookahead, 1, lookahead.length);
                    return new OptimisedInEdgeIterator(owner, bufferedEdgeIterator(ROLEPLAYER, mergedLookahead), encoding, roleType);
                }
            }

            public static class Out extends Buffered implements com.vaticle.typedb.core.graph.adjacency.ThingAdjacency.Write.Out {

                public Out(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new OutEdgeIterator(owner, bufferedEdgeIterator(encoding, lookahead), encoding);
                }

                @Override
                public OptimisedOutEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookahead) {
                    IID[] mergedLookahead = new IID[1 + lookahead.length];
                    mergedLookahead[0] = roleType.iid();
                    System.arraycopy(lookahead, 0, mergedLookahead, 1, lookahead.length);
                    return new OptimisedOutEdgeIterator(owner, bufferedEdgeIterator(ROLEPLAYER, mergedLookahead), encoding, roleType);
                }

            }
        }

        public static abstract class Persisted extends ThingAdjacencyImpl.Write {

            Persisted(ThingVertex.Write owner) {
                super(owner);
            }

            @Override
            public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
                return new UnsortedEdgeIterator(edgeIterator(encoding));
            }

            private FunctionalIterator<ThingEdge> edgeIterator(Encoding.Edge.Thing encoding, IID... lookahead) {
                Key.Prefix<EdgeIID.Thing> prefix = EdgeIID.Thing.prefix(owner.iid(), directedInfixIID(encoding, lookahead));
                FunctionalIterator<ThingEdge> storageIterator = owner.graph().storage().iterate(prefix, ASC)
                        .map(keyValue -> cache(newPersistedEdge(EdgeIID.Thing.of(keyValue.key().bytes()))));
                FunctionalIterator<ThingEdge> bufferedIterator = bufferedEdgeIterator(encoding, lookahead).map(ComparableEdge.Thing::edge);
                return link(bufferedIterator, storageIterator).distinct();
            }

            SortedIterator.Seekable<ComparableEdge<ThingEdge, EdgeIID.Thing>, Order.Asc> edgeIteratorSorted(Encoding.Edge.Thing encoding,
                                                                                                            IID... lookahead) {
                assert encoding != ROLEPLAYER || lookahead.length >= 1;
                Key.Prefix<EdgeIID.Thing> prefix = EdgeIID.Thing.prefix(owner.iid(), directedInfixIID(encoding, lookahead));
                SortedIterator.Seekable<ComparableEdge.Thing, Order.Asc> storageIter = owner.graph().storage().iterate(prefix, ASC)
                        .mapSorted(
                                ASC,
                                kv -> toComparable(cache(newPersistedEdge(EdgeIID.Thing.of(kv.key().bytes())))),
                                comparableEdge -> KeyValue.of(comparableEdge.iid(), ByteArray.empty())
                        );
                SortedIterator.Seekable<ComparableEdge.Thing, Order.Asc> bufferedIter = bufferedEdgeIterator(encoding, lookahead);
                return bufferedIter.merge(storageIter).distinct();
            }

            private ThingEdgeImpl.Persisted newPersistedEdge(EdgeIID.Thing of) {
                return new ThingEdgeImpl.Persisted(owner.graph(), of);
            }

            @Override
            public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
                assert !encoding.isOptimisation();
                ThingEdge edge = super.edge(encoding, adjacent);
                if (edge != null) return edge;

                EdgeIID.Thing edgeIID = EdgeIID.Thing.of(owner.iid(), directedInfixIID(encoding), adjacent.iid());
                if (owner.graph().storage().get(edgeIID) == null) return null;
                else return cache(newPersistedEdge(edgeIID));
            }

            @Override
            public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
                assert encoding.isOptimisation();
                ThingEdge edge = super.edge(encoding, adjacent, optimised);
                if (edge != null) return edge;

                EdgeIID.Thing edgeIID = EdgeIID.Thing.of(
                        owner.iid(), directedInfixIID(encoding, optimised.iid().type()),
                        adjacent.iid(), SuffixIID.of(optimised.iid().key())
                );
                if (owner.graph().storage().get(edgeIID) == null) return null;
                else return cache(newPersistedEdge(edgeIID));
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                edgeIterator(encoding).forEachRemaining(Edge::delete);
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                edgeIterator(encoding, lookAhead).forEachRemaining(Edge::delete);
            }

            public static class In extends Persisted implements com.vaticle.typedb.core.graph.adjacency.ThingAdjacency.Write.In {

                public In(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new InEdgeIterator(owner, edgeIteratorSorted(encoding, lookahead), encoding);
                }

                @Override
                public OptimisedInEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookahead) {
                    IID[] mergedLookahead = new IID[1 + lookahead.length];
                    mergedLookahead[0] = roleType.iid();
                    System.arraycopy(lookahead, 0, mergedLookahead, 1, lookahead.length);
                    return new OptimisedInEdgeIterator(owner, edgeIteratorSorted(ROLEPLAYER, mergedLookahead), encoding, roleType);

                }
            }

            public static class Out extends Persisted implements com.vaticle.typedb.core.graph.adjacency.ThingAdjacency.Write.Out {

                public Out(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new OutEdgeIterator(owner, edgeIteratorSorted(encoding, lookahead), encoding);
                }

                @Override
                public OptimisedOutEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookahead) {
                    IID[] mergedLookahead = new IID[1 + lookahead.length];
                    mergedLookahead[0] = roleType.iid();
                    System.arraycopy(lookahead, 0, mergedLookahead, 1, lookahead.length);
                    return new OptimisedOutEdgeIterator(owner, edgeIteratorSorted(ROLEPLAYER, mergedLookahead), encoding, roleType);
                }
            }
        }
    }
}
