package com.example.grpcclient.service;
import com.example.grpcclient.domain.RemoteReply;
import com.example.grpcclient.domain.FinalReply;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.springframework.stereotype.Service;
@Service
public class FinishService {
    public CompletionStage<FinalReply> process(RemoteReply input) {
        return CompletableFuture.completedFuture(new FinalReply(input.value() + "|final"));
    }
}
