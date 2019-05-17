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

import static org.axonframework.extensions.tracing.SpanUtils.withMessageTags;

import brave.Tracing;
import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import org.axonframework.messaging.InterceptorChain;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MessageHandlerInterceptor;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.unitofwork.UnitOfWork;


/**
 * A {@link MessageHandlerInterceptor} which maps the {@link MetaData} to the {@link brave.propagation.TraceContext}.
 *
 * @author Christophe Bouhier
 * @since 4.0
 */
public class OpenTraceHandlerInterceptor implements MessageHandlerInterceptor<Message<?>> {

    private final Tracing tracing;

    /**
     * Initialize a {@link MessageHandlerInterceptor} implementation which uses the provided {@link Tracing} to map span
     * information from the {@link Message} its {@link MetaData} on a {@link brave.propagation.TraceContext}.
     *
     * @param tracing the {@link Tracing} used to set a {@link brave.propagation.TraceContext} on from a {@link Message}'s {@link MetaData}
     */
    public OpenTraceHandlerInterceptor(Tracing tracing) {
        this.tracing = tracing;
    }

    @Override
    public Object handle(UnitOfWork unitOfWork, InterceptorChain interceptorChain) throws Exception {
        MetaData metaData = unitOfWork.getMessage().getMetaData();

        String operationName = "handle" + SpanUtils.resolveType(unitOfWork.getMessage());
        Tracer.SpanBuilder spanBuilder;
        try {

            MapExtractor extractor = new MapExtractor(metaData);
            SpanContext parentSpan = tracing.extract(Format.Builtin.TEXT_MAP, extractor);

            if (parentSpan == null) {
                spanBuilder = tracing.buildSpan(operationName);
            } else {
                spanBuilder = tracing.buildSpan(operationName).asChildOf(parentSpan);
            }
        } catch (IllegalArgumentException e) {
            spanBuilder = tracing.buildSpan(operationName);
        }

        try (Scope scope = withMessageTags(spanBuilder, unitOfWork.getMessage()).withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_SERVER).startActive(false)) {
            //noinspection unchecked
            unitOfWork.onCleanup(u -> scope.span().finish());
            return interceptorChain.proceed();
        }
    }

}
