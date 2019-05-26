package org.axonframework.extensions.tracing;

import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;

import java.util.Map;

public class MapExtractor implements TraceContext.Extractor<Map<String, String>> {

    @Override
    public TraceContextOrSamplingFlags extract(Map<String, String> carrier) {
        return null;
    }
}
