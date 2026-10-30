package com.example.grpcclient.service;
import com.example.grpcclient.domain.Input;
import com.example.grpcclient.domain.Prepared;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.springframework.stereotype.Service;

@Service
public class PrepareService {
    private final List<Pending> pending = new ArrayList<>();
    private boolean held;

    public synchronized void hold() {
        held = true;
    }

    public CompletionStage<Void> release() {
        List<Pending> ready;
        synchronized (this) {
            held = false;
            ready = List.copyOf(pending);
            pending.clear();
        }
        return CompletableFuture.runAsync(() -> ready.forEach(item -> item.result().complete(
            new Prepared(item.input().value() + "|prepared"))));
    }

    public synchronized CompletionStage<Prepared> process(Input input) {
        if (held) {
            var result = new CompletableFuture<Prepared>();
            pending.add(new Pending(input, result));
            return result;
        }
        return CompletableFuture.supplyAsync(() -> new Prepared(input.value() + "|prepared"));
    }

    private record Pending(Input input, CompletableFuture<Prepared> result) {}
}
