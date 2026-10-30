package com.example.grpcclient.mapper;
import com.example.grpcclient.domain.Prepared;
import com.example.grpcclient.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;
@Component
public class PreparedMapper implements Mapper<Prepared, PipelineTypes.Prepared> {
    @Override public Prepared fromExternal(PipelineTypes.Prepared external) { return new Prepared(external.getValue()); }
    @Override public PipelineTypes.Prepared toExternal(Prepared domain) { if (domain.value().contains("request-map-failure")) throw new IllegalArgumentException("request mapper failed");
        return PipelineTypes.Prepared.newBuilder().setValue("wire:" + domain.value()).build(); }
}
