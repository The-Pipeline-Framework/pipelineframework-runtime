package org.pipelineframework.telemetry;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FilePipelineReplayExporterLifecycleTest {

    @TempDir Path tempDir;

    @Test
    void directoryExporterStartsNoFlushTaskUntilAControlEventIsAccepted() throws Exception {
        Path output = tempDir.resolve("replay");
        FilePipelineReplayExporter exporter = new FilePipelineReplayExporter(output);
        assertFalse(exporter.controlFlushScheduled());
        assertFalse(Files.exists(output));

        exporter.emitControlEvent("payments", Instant.now(), topology(), event());
        assertTrue(exporter.controlFlushScheduled());
        exporter.close();
        assertFalse(exporter.controlFlushScheduled());
        try (var files = Files.list(output)) {
            assertEquals(1, files.count());
        }
    }

    @Test
    void shutdownBeforeFirstEventNeverStartsFlushTask() throws Exception {
        Path output = tempDir.resolve("replay");
        FilePipelineReplayExporter exporter = new FilePipelineReplayExporter(output);
        exporter.close();
        exporter.emitControlEvent("payments", Instant.now(), topology(), event());

        assertFalse(exporter.controlFlushScheduled());
        assertFalse(Files.exists(output));
    }

    private static PipelineReplayTopology topology() {
        return new PipelineReplayTopology("payments", List.of(), List.of());
    }

    private static PipelineExecutionEvent event() {
        return new PipelineExecutionEvent(
            "trace", "span", "parent", "item", "payments", "Await", "provider",
            "unit_item_completed", 0d, 0d, 0L, "source", "await", "ONE_TO_MANY",
            List.of(), 0L, 0, "", "", Map.of());
    }
}
