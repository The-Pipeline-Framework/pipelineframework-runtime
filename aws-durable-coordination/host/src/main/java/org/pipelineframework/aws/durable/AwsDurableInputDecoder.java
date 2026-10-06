package org.pipelineframework.aws.durable;

/** Application-generated adapter for the compiled pipeline input type. */
@FunctionalInterface
public interface AwsDurableInputDecoder {
    Object decode(String inputJson);
}
