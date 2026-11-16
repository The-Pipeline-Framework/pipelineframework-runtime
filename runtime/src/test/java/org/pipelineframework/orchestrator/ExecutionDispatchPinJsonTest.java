package org.pipelineframework.orchestrator;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import static org.junit.jupiter.api.Assertions.*;

class ExecutionDispatchPinJsonTest {
    @Test
    void retainedExecutionJsonWithoutPinAndNewPinnedRecordAreReadable() throws Exception {
        var pin = new ExecutionDispatchPin("entry-original", "deployment-original");
        var record = new ExecutionRecord<>("tenant", "execution", "key", "pipeline", "contract", "release",
            ExecutionResultShape.SINGLE, ExecutionStatus.QUEUED, 0, 0, 0, "", 0, 100, "", "input", "", "result",
            "", "", 100, 100, 1000, 0, 0, "", ExecutionRedriveIntent.REPLAY, -1,
            Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.of("initial-v1"), Optional.of(pin));
        String json = PipelineJson.mapper().writeValueAsString(record);
        assertEquals(record, PipelineJson.mapper().readValue(json, ExecutionRecord.class));
        var legacy = (com.fasterxml.jackson.databind.node.ObjectNode) PipelineJson.mapper().readTree(json);
        legacy.remove("dispatchPin");
        assertTrue(PipelineJson.mapper().treeToValue(legacy, ExecutionRecord.class).dispatchPin().isEmpty());
    }

    @Test
    void retainedCreationCommandWithoutPinAndNewPinnedCommandAreReadable() throws Exception {
        var command = ExecutionDispatchPinPersistenceTest.command("json-pin", ExecutionDispatchPinPersistenceTest.PIN);
        String json = PipelineJson.mapper().writeValueAsString(command);
        assertEquals(command, PipelineJson.mapper().readValue(json, ExecutionCreateCommand.class));
        var legacy = (com.fasterxml.jackson.databind.node.ObjectNode) PipelineJson.mapper().readTree(json);
        legacy.remove("dispatchPin");
        assertTrue(PipelineJson.mapper().treeToValue(legacy, ExecutionCreateCommand.class).dispatchPin().isEmpty());
    }
}
