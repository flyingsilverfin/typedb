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
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Seekable;
import com.vaticle.typedb.core.graph.adjacency.ThingAdjacency;
import com.vaticle.typedb.core.graph.common.Encoding;
import com.vaticle.typedb.core.graph.common.Storage.Key;
import com.vaticle.typedb.core.graph.edge.Edge;
import com.vaticle.typedb.core.graph.edge.ThingEdge;
import com.vaticle.typedb.core.graph.edge.impl.ThingEdgeImpl;
import com.vaticle.typedb.core.graph.iid.EdgeViewIID;
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

public abstract class ThingAdjacencyImpl<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>> implements ThingAdjacency {

    InfixIID.Thing viewInfixIID(Encoding.Edge.Thing encoding, IID... lookAhead) {
        return isOut() ? InfixIID.Thing.of(encoding.forward(), lookAhead) : InfixIID.Thing.of(encoding.backward(), lookAhead);
    }

    abstract ThingVertex owner();

    // TODO make abstract
    EdgeViewIID.Thing viewIID(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
        return EdgeViewIID.Thing.of(owner().iid(), viewInfixIID(encoding), adjacent.iid());
    }

    EdgeViewIID.Thing viewIID(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
        return EdgeViewIID.Thing.of(
                owner().iid(), viewInfixIID(encoding, optimised.iid().type()),
                adjacent.iid(), SuffixIID.of(optimised.iid().key())
        );
    }

    Key.Prefix<EdgeViewIID.Thing> viewIIDPrefix(Encoding.Edge.Thing.Optimised encoding) {
        return EdgeViewIID.Thing.prefix(owner().iid(), viewInfixIID(encoding));
    }

    Key.Prefix<EdgeViewIID.Thing> viewIIDPrefix(Encoding.Edge.Thing encoding, IID... lookahead) {
        return EdgeViewIID.Thing.prefix(owner().iid(), viewInfixIID(encoding, lookahead));
    }

    ThingEdgeImpl.Persisted newPersistedEdge(EdgeViewIID.Thing iid) {
        return new ThingEdgeImpl.Persisted(owner().graph(), iid);
    }

    abstract EDGE_VIEW getView(ThingEdge edge);

    Seekable<EDGE_VIEW, Order.Asc> edgeIteratorPersisted(Encoding.Edge.Thing encoding,
                                                         IID... lookahead) {
        assert encoding != ROLEPLAYER || lookahead.length >= 1;
        Key.Prefix<EdgeViewIID.Thing> prefix = viewIIDPrefix(encoding, lookahead);
        return owner().graph().storage().iterate(prefix, ASC).mapSorted(
                ASC,
                kv -> getView(newPersistedEdge(EdgeViewIID.Thing.of(kv.key().bytes()))),
                edgeView -> KeyValue.of(edgeView.iid(), ByteArray.empty())
        );
    }

    public static abstract class Read<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>> extends ThingAdjacencyImpl<EDGE_VIEW> {

        final ThingVertex owner;

        Read(ThingVertex owner) {
            this.owner = owner;
        }

        @Override
        ThingVertex owner() {
            return owner;
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
            EdgeViewIID.Thing iid = viewIID(encoding, adjacent);
            if (owner.graph().storage().get(iid) == null) return null;
            else return newPersistedEdge(iid);
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            EdgeViewIID.Thing iid = viewIID(encoding, adjacent, optimised);
            if (owner.graph().storage().get(iid) == null) return null;
            else return newPersistedEdge(iid);
        }

        @Override
        public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
            Key.Prefix<EdgeViewIID.Thing> prefix = viewIIDPrefix(encoding);
            return new UnsortedEdgeIterator(owner.graph().storage().iterate(prefix, ASC)
                    .map(kv -> newPersistedEdge(EdgeViewIID.Thing.of(kv.key().bytes()))));
        }

        public static class In extends Read<ThingEdge.View.Backward> implements ThingAdjacency.In {

            public In(ThingVertex owner) {
                super(owner);
            }

            @Override
            public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookAhead) {
                return new ThingEdgeIterator.InEdgeIteratorImpl(owner, edgeIteratorPersisted(encoding, lookAhead), encoding);
            }

            @Override
            public InEdgeIterator.Optimised edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookAhead) {
                IID[] mergedLookahead = new IID[1 + lookAhead.length];
                mergedLookahead[0] = roleType.iid();
                System.arraycopy(lookAhead, 0, mergedLookahead, 1, lookAhead.length);
                return new ThingEdgeIterator.InEdgeIteratorImpl.Optimised(owner, edgeIteratorPersisted(encoding, mergedLookahead), encoding, roleType);
            }

            @Override
            ThingEdge.View.Backward getView(ThingEdge edge) {
                return edge.getBackward();
            }
        }

        public static class Out extends Read<ThingEdge.View.Forward> implements ThingAdjacency.Out {

            public Out(ThingVertex owner) {
                super(owner);
            }

            @Override
            public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookAhead) {
                return new ThingEdgeIterator.OutEdgeIteratorImpl(owner, edgeIteratorPersisted(encoding, lookAhead), encoding);
            }

            @Override
            public OutEdgeIterator.Optimised edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookAhead) {
                IID[] mergedLookahead = new IID[1 + lookAhead.length];
                mergedLookahead[0] = roleType.iid();
                System.arraycopy(lookAhead, 0, mergedLookahead, 1, lookAhead.length);
                return new ThingEdgeIterator.OutEdgeIteratorImpl.Optimised(owner, edgeIteratorPersisted(encoding, mergedLookahead), encoding, roleType);
            }

            @Override
            ThingEdge.View.Forward getView(ThingEdge edge) {
                return edge.getForward();
            }
        }
    }

    public static abstract class Write<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>> extends ThingAdjacencyImpl<EDGE_VIEW> implements ThingAdjacency.Write {

        final ThingVertex.Write owner;
        final ConcurrentMap<InfixIID.Thing, ConcurrentSet<InfixIID.Thing>> infixes;
        final ConcurrentMap<InfixIID.Thing, ConcurrentNavigableMap<EDGE_VIEW, ThingEdge>> edges;

        Write(ThingVertex.Write owner) {
            this.owner = owner;
            this.infixes = new ConcurrentHashMap<>();
            this.edges = new ConcurrentHashMap<>();
        }

        @Override
        ThingVertex owner() {
            return owner;
        }

        IID[] infixTails(ThingEdge edge) {
            if (edge.encoding().isOptimisation()) {
                if (isOut()) {
                    return new IID[]{edge.getForward().iid().infix().asRolePlayer().tail(), edge.toIID().prefix(), edge.toIID().type()};
                } else {
                    return new IID[]{edge.getBackward().iid().infix().asRolePlayer().tail(), edge.fromIID().prefix(), edge.fromIID().type()};
                }
            } else {
                if (isOut()) return new IID[]{edge.toIID().prefix(), edge.toIID().type()};
                else return new IID[]{edge.fromIID().prefix(), edge.fromIID().type()};
            }
        }

        Seekable<EDGE_VIEW, Order.Asc> bufferedEdgeIterator(Encoding.Edge.Thing encoding, IID[] lookahead) {
            ConcurrentNavigableMap<EDGE_VIEW, ThingEdge> result;
            InfixIID.Thing infixIID = viewInfixIID(encoding, lookahead);
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
                ConcurrentNavigableMap<EDGE_VIEW, ThingEdge> res;
                return (res = edges.get(iid)) != null ? iterateSorted(ASC, res.keySet()) : emptySorted();
            });
        }

        @Override
        public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
            assert encoding.isOptimisation();
            Predicate<ThingEdge> predicate = isOut()
                    ? e -> e.to().equals(adjacent) && e.getForward().iid().suffix().equals(SuffixIID.of(optimised.iid().key()))
                    : e -> e.from().equals(adjacent) && e.getBackward().iid().suffix().equals(SuffixIID.of(optimised.iid().key()));
            FunctionalIterator<EDGE_VIEW> iterator = bufferedEdgeIterator(
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
            FunctionalIterator<EDGE_VIEW> iterator = bufferedEdgeIterator(
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
                                  boolean isReflexive) {
            assert encoding.lookAhead() == infixes.length;
            InfixIID.Thing infixIID = viewInfixIID(encoding);
            for (int i = 0; i < encoding.lookAhead(); i++) {
                this.infixes.computeIfAbsent(infixIID, x -> new ConcurrentSet<>()).add(
                        infixIID = viewInfixIID(encoding, copyOfRange(infixes, 0, i + 1))
                );
            }

            edges.compute(infixIID, (iid, bufferedEdges) -> {
                EDGE_VIEW edgeView = getView(edge);
                if (bufferedEdges == null) {
                    bufferedEdges = new ConcurrentSkipListMap<>();
                    bufferedEdges.put(edgeView, edge);
                } else {
                    ThingEdge existingEdge = bufferedEdges.get(edgeView);
                    if (existingEdge == null) bufferedEdges.put(edgeView, edge);
                    else if (existingEdge.isInferred() && !edge.isInferred()) existingEdge.isInferred(false);
                }
                return bufferedEdges;
            });

            assert !owner.isDeleted();
            owner.setModified();
            if (isReflexive) {
                // TODO was this an unchecked cast before?
                if (isOut()) ((ThingAdjacencyImpl.Write<ThingEdge.View.Forward>) edge.to().ins()).putNonReflexive(edge);
                else ((ThingAdjacencyImpl.Write<ThingEdge.View.Backward>) edge.from().outs()).putNonReflexive(edge);
            }
            return edge;
        }

        private void putNonReflexive(ThingEdgeImpl edge) {
            put(edge.encoding(), edge, infixTails(edge), false);
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
            return put(encoding, edge, infixes, true);
        }

        @Override
        public ThingEdge put(Encoding.Edge.Thing encoding, ThingVertex.Write adjacent, ThingVertex.Write optimised, boolean isInferred) {
            assert encoding.isOptimisation();
            ThingEdgeImpl edge = isOut()
                    ? new ThingEdgeImpl.Buffered(encoding, owner, adjacent, optimised, isInferred)
                    : new ThingEdgeImpl.Buffered(encoding, adjacent, owner, optimised, isInferred);
            IID[] infixes = new IID[]{optimised.iid().type(), adjacent.iid().prefix(), adjacent.iid().type()};
            return put(encoding, edge, infixes, true);
        }

        @Override
        public void remove(ThingEdge edge) {
            InfixIID.Thing infixIID = viewInfixIID(edge.encoding(), infixTails(edge));
            if (edges.containsKey(infixIID)) {
                edges.get(infixIID).remove(getView(edge));
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

        public static abstract class Buffered<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>>
                extends ThingAdjacencyImpl.Write<EDGE_VIEW> {

            Buffered(ThingVertex.Write owner) {
                super(owner);
            }

            @Override
            public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
                return new UnsortedEdgeIterator(bufferedEdgeIterator(encoding, new IID[]{}).map(ThingEdge.View::edge));
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                bufferedEdgeIterator(encoding, new IID[0]).forEachRemaining(comparableEdge -> comparableEdge.edge().delete());
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                bufferedEdgeIterator(encoding, lookAhead).forEachRemaining(comparableEdge -> comparableEdge.edge().delete());
            }

            public static class In extends Buffered<ThingEdge.View.Backward>
                    implements com.vaticle.typedb.core.graph.adjacency.ThingAdjacency.Write.In {

                public In(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                ThingEdge.View.Backward getView(ThingEdge edge) {
                    return edge.getBackward();
                }

                @Override
                public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new ThingEdgeIterator.InEdgeIteratorImpl(owner, bufferedEdgeIterator(encoding, lookahead), encoding);
                }

                @Override
                public InEdgeIterator.Optimised edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookahead) {
                    IID[] mergedLookahead = new IID[1 + lookahead.length];
                    mergedLookahead[0] = roleType.iid();
                    System.arraycopy(lookahead, 0, mergedLookahead, 1, lookahead.length);
                    return new ThingEdgeIterator.InEdgeIteratorImpl.Optimised(owner, bufferedEdgeIterator(ROLEPLAYER, mergedLookahead), encoding, roleType);
                }
            }

            public static class Out extends Buffered<ThingEdge.View.Forward>
                    implements com.vaticle.typedb.core.graph.adjacency.ThingAdjacency.Write.Out {

                public Out(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                ThingEdge.View.Forward getView(ThingEdge edge) {
                    return edge.getForward();
                }

                @Override
                public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new ThingEdgeIterator.OutEdgeIteratorImpl(owner, bufferedEdgeIterator(encoding, lookahead), encoding);
                }

                @Override
                public OutEdgeIterator.Optimised edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookahead) {
                    IID[] mergedLookahead = new IID[1 + lookahead.length];
                    mergedLookahead[0] = roleType.iid();
                    System.arraycopy(lookahead, 0, mergedLookahead, 1, lookahead.length);
                    return new ThingEdgeIterator.OutEdgeIteratorImpl.Optimised(owner, bufferedEdgeIterator(ROLEPLAYER, mergedLookahead), encoding, roleType);
                }

            }
        }

        public static abstract class Persisted<EDGE_VIEW extends ThingEdge.View<EDGE_VIEW>>
                extends ThingAdjacencyImpl.Write<EDGE_VIEW> {

            Persisted(ThingVertex.Write owner) {
                super(owner);
            }

            @Override
            public UnsortedEdgeIterator edge(Encoding.Edge.Thing.Optimised encoding) {
                return new UnsortedEdgeIterator(edgeIteratorUnsorted(encoding));
            }

            private FunctionalIterator<ThingEdge> edgeIteratorUnsorted(Encoding.Edge.Thing encoding, IID... lookahead) {
                Key.Prefix<EdgeViewIID.Thing> prefix = viewIIDPrefix(encoding, lookahead);
                FunctionalIterator<ThingEdge> storageIterator = owner.graph().storage().iterate(prefix, ASC)
                        .map(keyValue -> newPersistedEdge(EdgeViewIID.Thing.of(keyValue.key().bytes())));
                FunctionalIterator<ThingEdge> bufferedIterator = bufferedEdgeIterator(encoding, lookahead).map(ThingEdge.View::edge);
                return link(bufferedIterator, storageIterator);
            }

            Seekable<EDGE_VIEW, Order.Asc> edgeIterator(Encoding.Edge.Thing encoding, IID... lookahead) {
                assert encoding != ROLEPLAYER || lookahead.length >= 1;
                Seekable<EDGE_VIEW, Order.Asc> storageIter = edgeIteratorPersisted(encoding, lookahead);
                Seekable<EDGE_VIEW, Order.Asc> bufferedIter = bufferedEdgeIterator(encoding, lookahead);
                return bufferedIter.merge(storageIter).distinct();
            }

            @Override
            public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent) {
                assert !encoding.isOptimisation();
                ThingEdge edge = super.edge(encoding, adjacent);
                if (edge != null) return edge;

                EdgeViewIID.Thing edgeIID = viewIID(encoding, adjacent);
                if (owner.graph().storage().get(edgeIID) == null) return null;
                else return newPersistedEdge(edgeIID);
            }

            @Override
            public ThingEdge edge(Encoding.Edge.Thing encoding, ThingVertex adjacent, ThingVertex optimised) {
                assert encoding.isOptimisation();
                ThingEdge edge = super.edge(encoding, adjacent, optimised);
                if (edge != null) return edge;

                EdgeViewIID.Thing edgeIID = viewIID(encoding, adjacent, optimised);
                if (owner.graph().storage().get(edgeIID) == null) return null;
                else return newPersistedEdge(edgeIID);
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding) {
                edgeIteratorUnsorted(encoding).forEachRemaining(Edge::delete);
            }

            @Override
            public void delete(Encoding.Edge.Thing encoding, IID... lookAhead) {
                edgeIteratorUnsorted(encoding, lookAhead).forEachRemaining(Edge::delete);
            }

            public static class In extends Persisted<ThingEdge.View.Backward>
                    implements com.vaticle.typedb.core.graph.adjacency.ThingAdjacency.Write.In {

                public In(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                ThingEdge.View.Backward getView(ThingEdge edge) {
                    return edge.getBackward();
                }

                @Override
                public InEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new ThingEdgeIterator.InEdgeIteratorImpl(owner, edgeIterator(encoding, lookahead), encoding);
                }

                @Override
                public InEdgeIterator.Optimised edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookahead) {
                    IID[] mergedLookahead = new IID[1 + lookahead.length];
                    mergedLookahead[0] = roleType.iid();
                    System.arraycopy(lookahead, 0, mergedLookahead, 1, lookahead.length);
                    return new ThingEdgeIterator.InEdgeIteratorImpl.Optimised(owner, edgeIterator(ROLEPLAYER, mergedLookahead), encoding, roleType);

                }
            }

            public static class Out extends Persisted<ThingEdge.View.Forward>
                    implements com.vaticle.typedb.core.graph.adjacency.ThingAdjacency.Write.Out {

                public Out(ThingVertex.Write owner) {
                    super(owner);
                }

                @Override
                ThingEdge.View.Forward getView(ThingEdge edge) {
                    return edge.getForward();
                }

                @Override
                public OutEdgeIterator edge(Encoding.Edge.Thing.Base encoding, IID... lookahead) {
                    return new ThingEdgeIterator.OutEdgeIteratorImpl(owner, edgeIterator(encoding, lookahead), encoding);
                }

                @Override
                public OutEdgeIterator.Optimised edge(Encoding.Edge.Thing.Optimised encoding, TypeVertex roleType, IID... lookahead) {
                    IID[] mergedLookahead = new IID[1 + lookahead.length];
                    mergedLookahead[0] = roleType.iid();
                    System.arraycopy(lookahead, 0, mergedLookahead, 1, lookahead.length);
                    return new ThingEdgeIterator.OutEdgeIteratorImpl.Optimised(owner, edgeIterator(ROLEPLAYER, mergedLookahead), encoding, roleType);
                }
            }
        }
    }
}
