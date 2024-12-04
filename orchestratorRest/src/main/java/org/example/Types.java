package org.example;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

public final class Types {

    private Types() {}

    private static final ObjectMapper JSON_OBJ_MAPPER = new ObjectMapper();
    private static final String TYPES_NAMESPACE = "greeter.types";

    public static final Type<IngressRequest> INGRESS_JSON_TYPE = SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "Greet"), JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, IngressRequest.class));

    public static final Type<EgressRecord> EGRESS_RECORD_JSON_TYPE = SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf("io.statefun.playground", "EgressRecord"), JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, EgressRecord.class));
    }
