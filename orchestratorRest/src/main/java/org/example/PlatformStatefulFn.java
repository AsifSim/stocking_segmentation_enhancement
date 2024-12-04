package org.example;

import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.EgressMessageBuilder;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.example.RestClient.callRestEndpoint;
import static org.example.Types.*;

final class PlatformStatefulFn implements StatefulFunction {

    static final TypeName TYPE = TypeName.typeNameOf("org.example", "greetings");
    static final ValueSpec<Integer> SEEN = ValueSpec.named("seen").withIntType();
    static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE).withSupplier(PlatformStatefulFn::new).withValueSpecs(SEEN).build();
    private static final TypeName PLAYGROUND_EGRESS = TypeName.typeNameOf("io.statefun.playground", "egress");
//    private static final Logger logger = LoggerFactory.getLogger(PlatformStatefulFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws JsonProcessingException {
        System.out.println("=========Inside apply");
//        logger.info("=============Inside Class {}, Function {}",this.getClass().getSimpleName(),(new Object() {}.getClass().getEnclosingMethod().getName()));
//        logger.info("Parameter(context = {}, message = {})",context.toString(), message.toString());
        IngressRequest reqBody=message.as(INGRESS_JSON_TYPE);
        String response = callRestEndpoint("http://host.docker.internal:8099/stock-enhance-ui?partNumber="+reqBody.getPayload().get(0).get("part_number"));
//        logger.info("response = {}",response.toString());
        System.out.println("=========response="+response.toString());
        ObjectMapper objectMapper = new ObjectMapper();
        WorkflowResponse genericResponse = objectMapper.readValue(response, WorkflowResponse.class);
        if(genericResponse.getException()==null && genericResponse.getOperation()==null){
            final EgressRecord egressRecord=new EgressRecord("greetings",objectMapper.writeValueAsString(genericResponse.getValues()),context.self().id());
            context.send(EgressMessageBuilder.forEgress(PLAYGROUND_EGRESS).withCustomType(EGRESS_RECORD_JSON_TYPE, egressRecord).build());
//            logger.info("Have sent the message to egress");
        }
//        logger.info("=============Going out of {}",(new Object() {}.getClass().getEnclosingMethod().getName()));
        return context.done();
    }
}
