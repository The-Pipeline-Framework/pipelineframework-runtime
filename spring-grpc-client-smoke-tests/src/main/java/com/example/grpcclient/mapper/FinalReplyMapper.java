package com.example.grpcclient.mapper;
import com.example.grpcclient.domain.FinalReply;
import com.example.grpcclient.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;
@Component
public class FinalReplyMapper implements Mapper<FinalReply, PipelineTypes.FinalReply> {
    @Override public FinalReply fromExternal(PipelineTypes.FinalReply external) { return new FinalReply(external.getValue()); }
    @Override public PipelineTypes.FinalReply toExternal(FinalReply domain) { return PipelineTypes.FinalReply.newBuilder().setValue(domain.value()).build(); }
}
