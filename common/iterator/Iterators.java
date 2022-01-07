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

package com.vaticle.typedb.core.common.iterator;

import com.vaticle.typedb.common.collection.Either;
import com.vaticle.typedb.core.common.iterator.sorted.BasedSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.DistinctSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.FilteredSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.FinaliseSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.MappedSortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.MergeMappedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Forwardable;
import com.vaticle.typedb.core.common.iterator.sorted.SortedIterator.Order;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.vaticle.typedb.common.collection.Collections.list;
import static com.vaticle.typedb.common.collection.Collections.set;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterators.spliteratorUnknownSize;

public class Iterators {

    public static <T> FunctionalIterator<T> empty() {
        return iterate(set());
    }

    public static <T> FunctionalIterator<T> single(T item) {
        return iterate(set(item));
    }

    @SafeVarargs
    public static <T> FunctionalIterator<T> iterate(T... elements) {
        return iterate(list(elements));
    }

    public static <T> FunctionalIterator<T> iterate(Collection<T> collection) {
        return new BaseIterator<>(Either.second(collection.iterator()));
    }

    public static <T> FunctionalIterator<T> iterate(Iterator<T> iterator) {
        return new BaseIterator<>(Either.second(iterator));
    }

    public static <T> FunctionalIterator<T> link(Iterator<? extends T> iter1, Iterator<? extends T> iter2) {
        return link(list(iter1, iter2));
    }

    public static <T> FunctionalIterator<T> link(Iterator<? extends T> iter1, Iterator<? extends T> iter2,
                                                 Iterator<? extends T> iter3) {
        return link(list(iter1, iter2, iter3));
    }

    @SuppressWarnings("unchecked")
    public static <T> FunctionalIterator<T> link(List<? extends Iterator<? extends T>> iterators) {
        List<FunctionalIterator<T>> converted = new ArrayList<>();
        iterators.forEach(iterator -> {
            if (iterator instanceof AbstractFunctionalIterator<?>) {
                converted.add((FunctionalIterator<T>) iterator);
            } else {
                converted.add(iterate((Iterator<T>) iterator));
            }
        });
        return new LinkedIterators<>(converted);
    }

    public static <T> FunctionalIterator<T> loop(T seed, Predicate<T> predicate, UnaryOperator<T> function) {
        return new LoopIterator<>(seed, predicate, function);
    }

    public static <T> FunctionalIterator<T> tree(T root, Function<T, FunctionalIterator<T>> childrenFn) {
        return new TreeIterator<>(root, childrenFn);
    }

    public static <T> FunctionalIterator<List<T>> cartesian(List<FunctionalIterator<T>> iteratorProducers) {
        return new CartesianIterator<>(iteratorProducers);
    }

    public static <T> FunctionalIterator<List<T>> permutation(Collection<T> list) {
        return new PermutationIterator<>(list);
    }

    public static <T> Stream<T> stream(Iterator<T> iterator) {
        return StreamSupport.stream(spliteratorUnknownSize(iterator, ORDERED | IMMUTABLE), false);
    }


    public static int compareSize(Iterator<?> iterator, int size) {
        long count = 0L;
        while (iterator.hasNext()) {
            iterator.next();
            count++;
            if (count > size) return 1;
        }
        return count == size ? 0 : -1;
    }

    public static class Sorted {

        public static <T extends Comparable<T>> Forwardable<T, Order.Asc> emptySorted() {
            return iterateSorted(SortedIterator.ASC, new ConcurrentSkipListSet<T>());
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> Forwardable<T, ORDER> iterateSorted(ORDER order, NavigableSet<T> set) {
            return new BasedSortedIterator<>(order, set);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> distinct(
                SortedIterator<T, ORDER> iterator) {
            return new DistinctSortedIterator<>(iterator);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> Forwardable<T, ORDER> distinct(Forwardable<T, ORDER> iterator) {
            return new DistinctSortedIterator.Forwardable<>(iterator);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> SortedIterator<T, ORDER> filter(
                SortedIterator<T, ORDER> iterator, Predicate<T> predicate) {
            return new FilteredSortedIterator<>(iterator, predicate);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> Forwardable<T, ORDER> filter(Forwardable<T, ORDER> iterator,
                                                                                                          Predicate<T> predicate) {
            return new FilteredSortedIterator.Forwardable<>(iterator, predicate);
        }

        public static <T extends Comparable<? super T>, U extends Comparable<? super U>, ORDER extends Order>
        SortedIterator<U, ORDER> mapSorted(ORDER order, SortedIterator<T, ?> iterator, Function<T, U> mappingFn) {
            return new MappedSortedIterator<>(order, iterator, mappingFn);
        }

        public static <T extends Comparable<? super T>, U extends Comparable<? super U>, ORDER extends Order>
        Forwardable<U, ORDER> mapSorted(ORDER order, Forwardable<T, ?> iterator, Function<T, U> mappingFn, Function<U, T> reverseMappingFn) {
            return new MappedSortedIterator.Forwardable<>(order, iterator, mappingFn, reverseMappingFn);
        }

        @SafeVarargs
        public static <T extends Comparable<? super T>, ORDER extends Order> Forwardable<T, ORDER> merge(Forwardable<T, ORDER> iterator, Forwardable<T, ORDER>... iterators) {
            return new MergeMappedIterator.Forwardable<>(iterator.order(), iterate(list(list(iterators), iterator)), e -> e);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> Forwardable<T, ORDER> merge(ORDER order, FunctionalIterator<Forwardable<T, ORDER>> iterators) {
            return new MergeMappedIterator.Forwardable<>(order, iterators, e -> e);
        }

        public static <T extends Comparable<? super T>, ORDER extends Order> Forwardable<T, ORDER> onFinalise(Forwardable<T, ORDER> iterator,
                                                                                                              Runnable finalise) {
            return new FinaliseSortedIterator.Forwardable<>(iterator, finalise);
        }
    }
}
