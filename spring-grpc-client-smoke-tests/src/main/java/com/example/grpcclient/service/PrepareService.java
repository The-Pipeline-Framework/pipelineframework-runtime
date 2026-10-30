package com.example.grpcclient.service;
import com.example.grpcclient.domain.Input;
import com.example.grpcclient.domain.Prepared;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.springframework.stereotype.Service;
@Service
public class PrepareService {
    public CompletionStage<Prepared> process(Input input) {
        return CompletableFuture.supplyAsync(() -> new Prepared(input.value() + "|prepared"));
    }
}
