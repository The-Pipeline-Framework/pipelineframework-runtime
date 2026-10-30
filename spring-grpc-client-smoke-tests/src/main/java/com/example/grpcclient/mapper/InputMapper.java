package com.example.grpcclient.mapper;
import com.example.grpcclient.domain.Input;
import com.example.grpcclient.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;
@Component
public class InputMapper implements Mapper<Input, PipelineTypes.Input> {
    @Override public Input fromExternal(PipelineTypes.Input external) { return new Input(external.getValue()); }
    @Override public PipelineTypes.Input toExternal(Input domain) { return PipelineTypes.Input.newBuilder().setValue(domain.value()).build(); }
}
