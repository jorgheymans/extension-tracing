/*
 * Copyright (c) 2010-2019. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.axonframework.extensions.tracing;

import brave.Span;
import brave.Tracer;
import brave.Tracing;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.axonframework.messaging.responsetypes.ResponseType;
import org.axonframework.queryhandling.DefaultQueryGateway;
import org.axonframework.queryhandling.QueryBus;
import org.axonframework.queryhandling.QueryMessage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * A tracing query gateway which activates a calling {@link brave.Span}, when the {@link CompletableFuture} completes.
 *
 * @author Christophe Bouhier
 * @since 4.0
 */
public class TracingQueryGateway extends DefaultQueryGateway {

    private final Tracing tracing;

    /**
     * Instantiate a Builder to be able to create a {@link TracingQueryGateway}.
     * <p>
     * The {@code dispatchInterceptors} are defaulted to an empty list.
     * The {@link Tracing} and {@link QueryBus} are <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@link TracingQueryGateway}
     */
    public static Builder builder() {
        return new Builder();
    }

    private TracingQueryGateway(Builder builder) {
        super(builder);
        this.tracing = builder.tracer;
    }

    @Override
    public <R, Q> CompletableFuture<R> query(String queryName, Q query, ResponseType<R> responseType) {
        Span newSpan = tracing.tracer().nextSpan().name(queryName).start();
        try (Tracer.SpanInScope ignored = tracing.tracer().withSpanInScope(newSpan)) {
            return super.query(queryName, query, responseType).whenComplete((r, e) -> newSpan.finish());
        }
    }

    /**
     * Builder class to instantiate a {@link TracingQueryGateway}.
     * <p>
     * The {@code dispatchInterceptors} are defaulted to an empty list.
     * The {@link Tracing} and {@link QueryBus} are <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder extends DefaultQueryGateway.Builder {

        private Tracing tracer;

        @Override
        public Builder queryBus(QueryBus queryBus) {
            super.queryBus(queryBus);
            return this;
        }

        @Override
        public Builder dispatchInterceptors(
                MessageDispatchInterceptor<? super QueryMessage<?, ?>>... dispatchInterceptors) {
            super.dispatchInterceptors(dispatchInterceptors);
            return this;
        }

        @Override
        public Builder dispatchInterceptors(
                List<MessageDispatchInterceptor<? super QueryMessage<?, ?>>> dispatchInterceptors) {
            super.dispatchInterceptors(dispatchInterceptors);
            return this;
        }

        /**
         * Sets the {@link Tracing} used to set a {@link brave.Span} on dispatched {@link QueryMessage}s.
         *
         * @param tracing a {@link Tracing} used to set a {@link brave.Span} on dispatched {@link QueryMessage}s.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder tracer(Tracing tracing) {
            assertNonNull(tracing, "Tracing may not be null");
            this.tracer = tracing;
            return this;
        }

        /**
         * Initializes a {@link TracingQueryGateway} as specified through this Builder.
         *
         * @return a {@link TracingQueryGateway} as specified through this Builder
         */
        public TracingQueryGateway build() {
            return new TracingQueryGateway(this);
        }
    }
}
