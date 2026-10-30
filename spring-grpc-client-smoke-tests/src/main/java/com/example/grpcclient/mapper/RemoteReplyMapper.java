package com.example.grpcclient.mapper;
import com.example.grpcclient.domain.RemoteReply;
import com.example.grpcclient.grpc.PipelineTypes;
import org.pipelineframework.mapper.Mapper;
import org.springframework.stereotype.Component;
@Component
public class RemoteReplyMapper implements Mapper<RemoteReply, PipelineTypes.RemoteReply> {
    @Override public RemoteReply fromExternal(PipelineTypes.RemoteReply external) { if (external.getValue().contains("response-map-failure")) throw new IllegalArgumentException("response mapper failed");
        return new RemoteReply(external.getValue() + "|mapped"); }
    @Override public PipelineTypes.RemoteReply toExternal(RemoteReply domain) { return PipelineTypes.RemoteReply.newBuilder().setValue(domain.value()).build(); }
}
