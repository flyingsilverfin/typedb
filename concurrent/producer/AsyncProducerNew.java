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

package com.vaticle.typedb.core.concurrent.producer;

import com.vaticle.typedb.common.collection.ConcurrentSet;
import com.vaticle.typedb.common.collection.Triple;
import com.vaticle.typedb.core.common.iterator.FunctionalIterator;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Predicate;

@ThreadSafe
public class AsyncProducerNew<T> implements FunctionalProducer<T> {

    private final int parallelisation;
    private final ProduceManager workManager;
    private final IteratorManager iteratorManager;
    private int workers;
    private volatile boolean done;

    AsyncProducerNew(FunctionalIterator<FunctionalIterator<T>> iteratorSource, int parallelisation) {
        assert parallelisation > 0;
        this.parallelisation = parallelisation;
        this.workManager = new ProduceManager();
        this.iteratorManager = new IteratorManager(iteratorSource);
        this.workers = 0;
        this.done = false;
    }

    @Override
    public <U> AsyncProducerNew<U> map(Function<T, U> mappingFn) {
        return new AsyncProducerNew<>(iteratorManager.iteratorSource.map(iter -> iter.map(mappingFn)), parallelisation);
    }

    @Override
    public AsyncProducerNew<T> filter(Predicate<T> predicate) {
        return new AsyncProducerNew<>(iteratorManager.iteratorSource.map(iter -> iter.filter(predicate)), parallelisation);
    }

    @Override
    public AsyncProducerNew<T> distinct() {
        ConcurrentSet<T> produced = new ConcurrentSet<>();
        return new AsyncProducerNew<>(iteratorManager.iteratorSource.map(iter -> iter.distinct(produced)), parallelisation);
    }

    @Override
    public void produce(Queue<T> queue, int request, Executor executor) {
        workManager.add(queue, request, executor);
        if (!iteratorManager.hasIterator()) {
            executor.execute(queue::done);
            return;
        }
        synchronized (this) {
            int availableWorkers = parallelisation - workers;
            for (int i = 0; i < availableWorkers && workManager.hasNext(); i++) {
                createWorker();
            }
        }
    }

    private synchronized void createWorker() {
        assert workManager.hasNext();
        Optional<Triple<Queue<T>, Integer, Executor>> next = workManager.next();
        CompletableFuture.runAsync(() -> fulfill(next.get().first(), next.get().second()), next.get().third());
        workers++;
    }

    private void fulfill(Queue<T> queue, int requested) {
        assert requested > 0;
        FunctionalIterator<T> iter = null;
        for (int i = 0; i < requested && !done; i++) {
            if (iter == null || !iter.hasNext()) {
                if (iter != null) iteratorManager.unreserveFinished(iter);
                Optional<FunctionalIterator<T>> newIterator = iteratorManager.reserve();
                if (!newIterator.isPresent()) break;
                iter = newIterator.get();
            }
            queue.put(iter.next());
        }
        iteratorManager.unreserve(iter);
        workerFinished(queue);
    }

    synchronized void workerFinished(Queue<T> queue) {
        workers--;
        if (workers == 0 && !iteratorManager.hasIterator() && !done) {
            queue.done();
            done = true;
        } else if (iteratorManager.hasIterator() && workManager.hasNext()) {
            createWorker();
        }
    }

    private class IteratorManager {

        private final FunctionalIterator<FunctionalIterator<T>> iteratorSource;
        private final Set<FunctionalIterator<T>> reserved;
        private final List<FunctionalIterator<T>> available;

        IteratorManager(FunctionalIterator<FunctionalIterator<T>> iteratorSource) {
            this.iteratorSource = iteratorSource;
            this.reserved = new HashSet<>();
            this.available = new LinkedList<>();
        }

        private synchronized boolean hasIterator() {
            return !available.isEmpty() || iteratorSource.hasNext();
        }

        private synchronized Optional<FunctionalIterator<T>> reserve() {
            if (!available.isEmpty()) return Optional.of(available.remove(0));
            else if (iteratorSource.hasNext()) {
                FunctionalIterator<T> next = iteratorSource.next();
                reserved.add(next);
                return Optional.of(next);
            } else return Optional.empty();
        }

        private synchronized void unreserve(FunctionalIterator<T> iterator) {
            if (iterator != null) {
                reserved.remove(iterator);
                if (iterator.hasNext()) available.add(iterator);
            }
        }

        public synchronized void unreserveFinished(FunctionalIterator<T> iterator) {
            reserved.remove(iterator);
        }

        public synchronized void recycleIterators() {
            iteratorSource.recycle();
            reserved.forEach(FunctionalIterator::recycle);
            available.forEach(FunctionalIterator::recycle);
        }
    }

    private class ProduceManager {
        private final List<Triple<Queue<T>, Integer, Executor>> toProduce;
        private int totalToProduce;

        ProduceManager() {
            toProduce = new LinkedList<>();
            totalToProduce = 0;
        }

        void add(Queue<T> queue, int request, Executor executor) {
            toProduce.add(new Triple<>(queue, request, executor));
            totalToProduce += request;
        }

        Optional<Triple<Queue<T>, Integer, Executor>> next() {
            if (toProduce.isEmpty()) return Optional.empty();
            Triple<Queue<T>, Integer, Executor> next = toProduce.remove(0);
            int batch = totalToProduce / parallelisation;
            if (batch >= next.second()) {
                return Optional.of(next);
            } else {
                toProduce.add(0, new Triple<>(next.first(), next.second() - batch, next.third()));
                return Optional.of(new Triple<>(next.first(), batch, next.third()));
            }
        }

        boolean hasNext() {
            return !toProduce.isEmpty();
        }
    }

    @Override
    public synchronized void recycle() {
        done = true;
        iteratorManager.recycleIterators();
    }
//
//    private synchronized void initialise(Queue<T> queue) {
//        for (int i = 0; i < parallelisation && iterators.hasNext(); i++) {
//            runningJobs.put(iterators.next(), completedFuture(null));
//        }
//        isInitialised = true;
//        if (runningJobs.isEmpty()) done(queue);
//    }
//
//    private synchronized void distribute(Queue<T> queue, int request, Executor executor) {
//        if (isDone.get()) return;
//        int requestSplitMax = (int) Math.ceil((double) request / runningJobs.size());
//        int requestSent = 0;
//        for (FunctionalIterator<T> iterator : runningJobs.keySet()) {
//            int requestSplit = Math.min(requestSplitMax, request - requestSent);
//            runningJobs.computeIfPresent(iterator, (iter, asyncJob) -> asyncJob.thenRunAsync(
//                    () -> job(queue, iter, requestSplit, executor), executor
//            ));
//            requestSent += requestSplit;
//            if (requestSent == request) break;
//        }
//    }
//
//    private synchronized void transition(Queue<T> queue, FunctionalIterator<T> iterator, int unfulfilled, Executor executor) {
//        if (!iterator.hasNext()) {
//            if (runningJobs.remove(iterator) != null && iterators.hasNext()) compensate(queue, unfulfilled, executor);
//            else if (!runningJobs.isEmpty() && unfulfilled > 0) distribute(queue, unfulfilled, executor);
//            else if (runningJobs.isEmpty()) done(queue);
//            else if (unfulfilled != 0) throw TypeDBException.of(ILLEGAL_STATE);
//        } else {
//            if (unfulfilled != 0) throw TypeDBException.of(ILLEGAL_STATE);
//        }
//    }
//
//    private synchronized void compensate(Queue<T> queue, int unfulfilled, Executor executor) {
//        FunctionalIterator<T> it = iterators.next();
//        runningJobs.put(it, completedFuture(null));
//        if (unfulfilled > 0) {
//            runningJobs.computeIfPresent(it, (i, job) -> job.thenRunAsync(
//                    () -> job(queue, it, unfulfilled, executor), executor
//            ));
//        }
//    }
//
//    private synchronized void job(Queue<T> queue, FunctionalIterator<T> iterator, int request, Executor executor) {
//        if (!runningJobs.containsKey(iterator)) return;
//        try {
//            int unfulfilled = request;
//            if (runningJobs.containsKey(iterator)) {
//                for (; unfulfilled > 0 && iterator.hasNext() && !isDone.get(); unfulfilled--) {
//                    queue.put(iterator.next());
//                }
//            }
//            if (!isDone.get()) transition(queue, iterator, unfulfilled, executor);
//        } catch (Throwable e) {
//            done(queue, e);
//        }
//    }
//

}
