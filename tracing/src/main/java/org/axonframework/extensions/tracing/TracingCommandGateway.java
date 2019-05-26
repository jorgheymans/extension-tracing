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
import brave.Tracer.SpanInScope;
import brave.Tracing;
import org.axonframework.commandhandling.*;
import org.axonframework.commandhandling.callbacks.FutureCallback;
import org.axonframework.commandhandling.gateway.DefaultCommandGateway;
import org.axonframework.commandhandling.gateway.RetryScheduler;
import org.axonframework.messaging.MessageDispatchInterceptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.axonframework.common.BuilderUtils.assertNonNull;

/**
 * A tracing command gateway which activates a calling {@link brave.Span}, when the {@link CompletableFuture} completes.
 *
 * @author Christophe Bouhier
 * @author Allard Buijze
 * @since 4.0
 */
public class TracingCommandGateway extends DefaultCommandGateway {

    private final Tracing tracing;

    private TracingCommandGateway(Builder builder) {
        super(builder);
        this.tracing = builder.tracer;
    }

    /**
     * Instantiate a Builder to be able to create a {@link TracingCommandGateway}.
     * <p>
     * The {@code dispatchInterceptors} are defaulted to an empty list.
     * The {@link Tracing} and {@link CommandBus} are <b>hard requirements</b> and as such should be provided.
     *
     * @return a Builder to be able to create a {@link TracingCommandGateway}
     */
    public static Builder builder() {
        return new Builder();
    }

    public <C, R> void send(C command, CommandCallback<? super C, ? super R> callback) {
        CommandMessage<?> cmd = GenericCommandMessage.asCommandMessage(command);
        sendWithSpan(tracing, "sendCommandMessage", cmd, (tracer, parentSpan, childSpan) -> {
            CompletableFuture<?> resultReceived = new CompletableFuture<>();
            super.send(command, (CommandCallback<C, R>) (commandMessage, commandResultMessage) -> {
                try (SpanInScope ignored = tracer.tracer().withSpanInScope(parentSpan)) {
                    childSpan.annotate("resultReceived");
                    callback.onResult(commandMessage, commandResultMessage);
                    childSpan.annotate("afterCallbackInvocation");
                } finally {
                    resultReceived.complete(null);
                }
            });
            childSpan.annotate("dispatchComplete");
            resultReceived.thenRun(childSpan::finish);
        });
    }

    @Override
    public <R> R sendAndWait(Object command) {
        return doSendAndExtract(command, FutureCallback::getResult);
    }

    @Override
    public <R> R sendAndWait(Object command, long timeout, TimeUnit unit) {
        return doSendAndExtract(command, f -> f.getResult(timeout, unit));
    }

    private <R> R doSendAndExtract(Object command,
                                   Function<FutureCallback<Object, R>, CommandResultMessage<? extends R>> resultExtractor) {
        FutureCallback<Object, R> futureCallback = new FutureCallback<>();

        sendAndRestoreParentSpan(command, futureCallback);
        CommandResultMessage<? extends R> commandResultMessage = resultExtractor.apply(futureCallback);
        if (commandResultMessage.isExceptional()) {
            throw asRuntime(commandResultMessage.exceptionResult());
        }
        return commandResultMessage.getPayload();
    }

    private <R> void sendAndRestoreParentSpan(Object command, FutureCallback<Object, R> futureCallback) {
        CommandMessage<?> cmd = GenericCommandMessage.asCommandMessage(command);
        sendWithSpan(tracing, "sendCommandMessageAndWait", cmd, (tracer, parentSpan, childSpan) -> {
            super.send(cmd, futureCallback);
            futureCallback.thenRun(() -> childSpan.annotate("resultReceived"));

            childSpan.annotate("dispatchComplete");
            futureCallback.thenRun(childSpan::finish);
        });
    }

    private RuntimeException asRuntime(Throwable e) {
        Throwable failure = e.getCause();
        if (failure instanceof Error) {
            throw (Error) failure;
        } else if (failure instanceof RuntimeException) {
            return (RuntimeException) failure;
        } else {
            return new CommandExecutionException("An exception occurred while executing a command", failure);
        }
    }

    private void sendWithSpan(Tracing tracing, String operation, CommandMessage<?> command, SpanConsumer consumer) {
        Span parent = tracing.tracer().currentSpan();
        final Span newSpan = tracing.tracer().nextSpan().kind(Span.Kind.CLIENT).name(operation);
        SpanUtils.withMessageTags(newSpan, command);
        try (SpanInScope ignored = tracing.tracer().withSpanInScope(newSpan)) {
            consumer.accept(tracing, parent, newSpan);
        } finally {
            newSpan.finish();
        }
    }

    @FunctionalInterface
    private interface SpanConsumer {

        void accept(Tracing tracer, Span activeSpan, Span parentSpan);
    }

    /**
     * Builder class to instantiate a {@link TracingCommandGateway}.
     * <p>
     * The {@code dispatchInterceptors} are defaulted to an empty list.
     * The {@link Tracer} and {@link CommandBus} are <b>hard requirements</b> and as such should be provided.
     */
    public static class Builder extends DefaultCommandGateway.Builder {

        private Tracing tracer;

        @Override
        public Builder commandBus(CommandBus commandBus) {
            super.commandBus(commandBus);
            return this;
        }

        @Override
        public Builder retryScheduler(RetryScheduler retryScheduler) {
            super.retryScheduler(retryScheduler);
            return this;
        }

        @Override
        public Builder dispatchInterceptors(
                MessageDispatchInterceptor<? super CommandMessage<?>>... dispatchInterceptors) {
            super.dispatchInterceptors(dispatchInterceptors);
            return this;
        }

        @Override
        public Builder dispatchInterceptors(
                List<MessageDispatchInterceptor<? super CommandMessage<?>>> dispatchInterceptors) {
            super.dispatchInterceptors(dispatchInterceptors);
            return this;
        }

        /**
         * Sets the {@link Tracing} used to set a {@link Span} on dispatched {@link CommandMessage}s.
         *
         * @param tracer a {@link Tracing} used to set a {@link Span} on dispatched {@link CommandMessage}s.
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder tracer(Tracing tracer) {
            assertNonNull(tracer, "Tracing may not be null");
            this.tracer = tracer;
            return this;
        }

        /**
         * Initializes a {@link TracingCommandGateway} as specified through this Builder.
         *
         * @return a {@link TracingCommandGateway} as specified through this Builder
         */
        public TracingCommandGateway build() {
            return new TracingCommandGateway(this);
        }
    }
}
