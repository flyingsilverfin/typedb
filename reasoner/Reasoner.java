/*
 * Copyright (C) 2020 Grakn Labs
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

package grakn.core.reasoner;

import grakn.core.common.async.Producer;
import grakn.core.common.concurrent.ExecutorService;
import grakn.core.common.iterator.ResourceIterator;
import grakn.core.concept.ConceptManager;
import grakn.core.concept.answer.ConceptMap;
import grakn.core.pattern.Conjunction;
import grakn.core.pattern.Disjunction;
import grakn.core.reasoner.resolution.ResolverRegistry;
import grakn.core.traversal.TraversalEngine;

import java.util.List;
import java.util.function.Predicate;

import static grakn.common.collection.Collections.list;
import static grakn.core.common.async.Producers.buffer;
import static java.util.stream.Collectors.toList;

public class Reasoner {

    private final TraversalEngine traversalEng;
    private final ConceptManager conceptMgr;
    private final ResolverRegistry resolverRegistry;

    public Reasoner(TraversalEngine traversalEng, ConceptManager conceptMgr) {
        this.traversalEng = traversalEng;
        this.conceptMgr = conceptMgr;
        this.resolverRegistry = new ResolverRegistry(ExecutorService.eventLoopGroup());
    }

    public ResourceIterator<ConceptMap> execute(Disjunction disjunction) {
        return buffer(disjunction.conjunctions().stream()
                              .flatMap(conjunction -> execute(conjunction).stream())
                              .collect(toList())).iterator();
    }

    public List<Producer<ConceptMap>> execute(Disjunction disjunction, ConceptMap bounds) {
        return disjunction.conjunctions().stream().flatMap(conj -> execute(conj, bounds).stream()).collect(toList());
    }

    public List<Producer<ConceptMap>> execute(Conjunction conjunction) {
        Conjunction conjunctionHinted = resolveTypes(conjunction);
        Producer<ConceptMap> answers = traversalEng.execute(conjunctionHinted.traversal()).map(conceptMgr::conceptMap);

        // TODO enable reasoner here
        //      ResourceIterator<ConceptMap> answers = link(list(
        //          traversalEng.execute(conjunctionResolvedTypes.traversal()).map(conceptMgr::conceptMap)
        //          resolve(conjunctionResolvedTypes)
        //      ));

        if (conjunctionHinted.negations().isEmpty()) {
            return list(answers);
        } else {
            Predicate<ConceptMap> predicate = answer -> !buffer(conjunctionHinted.negations().stream().flatMap(
                    negation -> execute(negation.disjunction(), answer).stream()
            ).collect(toList())).iterator().hasNext();
            return list(answers.filter(predicate));
        }
    }

    public List<Producer<ConceptMap>> execute(Conjunction conjunction, ConceptMap bounds) {
        return null; // TODO
    }

    private Conjunction resolveTypes(Conjunction conjunction) {
        // TODO implement Type Inference
        return conjunction;
    }

    private ReasonerProducer resolve(Conjunction conjunction) {
        // TODO get onAnswer and onDone callbacks
        //      return new ReasonerProducer(conjunction, resolverRegistry, null, null);
        return null;
    }
}
