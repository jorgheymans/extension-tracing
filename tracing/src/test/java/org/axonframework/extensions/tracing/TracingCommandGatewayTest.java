package org.axonframework.extensions.tracing;

import brave.ScopedSpan;
import brave.Tracing;
import brave.propagation.StrictScopeDecorator;
import brave.propagation.ThreadLocalCurrentTraceContext;
import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandCallback;
import org.axonframework.commandhandling.CommandMessage;
import org.junit.*;
import zipkin2.Span;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.axonframework.commandhandling.GenericCommandResultMessage.asCommandResultMessage;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

public class TracingCommandGatewayTest {

    private CommandBus mockCommandBus;
    private TracingCommandGateway testSubject;
    private List<Span> spans = new ArrayList<>();
    private Tracing tracing;

    @Before
    public void before() {
        tracing = Tracing
                .newBuilder()
                .localServiceName("axon-tracing")
                .currentTraceContext(ThreadLocalCurrentTraceContext.newBuilder().
                        addScopeDecorator(StrictScopeDecorator.create()).build())
                .spanReporter(spans::add)
                .build();
        mockCommandBus = mock(CommandBus.class);
        testSubject = TracingCommandGateway.builder()
                                           .commandBus(mockCommandBus)
                                           .tracer(tracing)
                                           .build();
    }

    @After
    public void close() {
        Tracing.current().close();
        spans.clear();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSendWithCallback() {
        doAnswer(invocation -> {
            ((CommandCallback) invocation.getArguments()[1])
                    .onResult((CommandMessage) invocation.getArguments()[0],
                              asCommandResultMessage("result"));
            return null;
        }).when(mockCommandBus).dispatch(isA(CommandMessage.class), isA(CommandCallback.class));

        final ScopedSpan newSpan = Tracing.currentTracer().startScopedSpan("test");

        testSubject.send("Command", (m, r) -> {
            // Call back.
            assertThat(r, notNullValue());
        });

        verify(mockCommandBus, times(1)).dispatch(isA(CommandMessage.class), isA(CommandCallback.class));
        newSpan.finish();

        assertThat(spans.size(), is(2));
        assertThat(spans.get(0).name(), is("sendcommandmessage"));
        assertThat(spans.get(0).tags().get(SpanUtils.TAG_AXON_COMMAND_NAME), is("java.lang.String"));
        assertThat(spans.get(0).tags().get(SpanUtils.TAG_AXON_MSG_TYPE), is("CommandMessage"));
        assertThat(spans.get(0).tags().get(SpanUtils.TAG_AXON_PAYLOAD_TYPE), is("java.lang.String"));
        assertThat(spans.get(0).tags().containsKey(SpanUtils.TAG_AXON_ID), is(true));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSendWithoutCallback() throws ExecutionException, InterruptedException {
        doAnswer(invocation -> {
            ((CommandCallback) invocation.getArguments()[1])
                    .onResult((CommandMessage) invocation.getArguments()[0],
                              asCommandResultMessage("result"));
            return null;
        }).when(mockCommandBus).dispatch(isA(CommandMessage.class), isA(CommandCallback.class));

        final ScopedSpan newSpan = tracing.tracer().startScopedSpan("test");

        CompletableFuture<Object> future = testSubject.send("Command");

        verify(mockCommandBus, times(1)).dispatch(isA(CommandMessage.class), isA(CommandCallback.class));

        newSpan.finish();
        System.out.println("[" + String.join(",", spans.stream().map(span -> span.toString()).collect(Collectors
                                                                                                              .toList()))
                                   + "]");
        assertThat(future.isDone(), is(true));
        assertThat(future.get(), is("result"));
        assertThat(spans.size(), is(2));
        assertThat(spans.get(0).name(), is("sendcommandmessage"));
        assertThat(spans.get(0).parentId(), is(spans.get(1).traceId()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSendAndWait() {
        doAnswer(invocation -> {
            ((CommandCallback) invocation.getArguments()[1])
                    .onResult((CommandMessage) invocation.getArguments()[0],
                              asCommandResultMessage("result"));
            return null;
        }).when(mockCommandBus).dispatch(isA(CommandMessage.class), isA(CommandCallback.class));

        final ScopedSpan newSpan = tracing.tracer().startScopedSpan("test");

        Object result = testSubject.sendAndWait("Command");

        verify(mockCommandBus, times(1)).dispatch(isA(CommandMessage.class), isA(CommandCallback.class));

        newSpan.finish();
        assertThat(result, instanceOf(String.class));
        assertThat(result, is("result"));
        System.out.println("[" + String.join(",", spans.stream().map(span -> span.toString()).collect(Collectors
                                                                                                              .toList()))
                                   + "]");
        assertThat(spans.size(), is(2));
        assertThat(spans.get(0).name(), is("sendcommandmessageandwait"));
        assertThat(spans.get(0).parentId(), is(spans.get(1).traceId()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testSendAndWaitWithTimeout() {

        final ScopedSpan newSpan = tracing.tracer().startScopedSpan("test");

        Object result = testSubject.sendAndWait("Command", 10, TimeUnit.MILLISECONDS);

        newSpan.finish();
        verify(mockCommandBus, times(1)).dispatch(isA(CommandMessage.class), isA(CommandCallback.class));

        System.out.println("[" + String.join(",", spans.stream().map(span -> span.toString()).collect(Collectors
                                                                                                              .toList()))
                                   + "]");
        assertThat(result, nullValue());


    }
}
