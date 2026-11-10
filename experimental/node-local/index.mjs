import { addAbortListener } from 'node:events';
import { isPromise } from 'node:util/types';
import { admitBinding } from './projection.mjs';
import { NodeLocalDiagnostic, requireKeys, snapshot } from './records.mjs';

export { NodeLocalDiagnostic };

const unclassified = 'Unclassified business function failure';
const readAborted = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted').get;

/** The only explicit host carrier for an application failure envelope. */
export class StructuredFailure extends Error {
  #envelope;

  constructor(envelope) {
    super('Structured business function failure');
    this.name = 'StructuredFailure';
    this.#envelope = envelope;
  }

  static bind(error, stepId) {
    // Branding avoids instanceof, which can invoke a thrown object's proxy traps.
    if (error === null || typeof error !== 'object' || !(#envelope in error)) {
      return Object.freeze({code: 'UNCLASSIFIED', message: unclassified,
        retryable: false, owner: 'LOCAL_SHELL', stepId});
    }

    const failure = snapshot(error.#envelope, 'MALFORMED_FAILURE');
    requireKeys(failure, ['code', 'message', 'retryable', 'owner', 'stepId'], 'MALFORMED_FAILURE');
    if (['code', 'message', 'owner', 'stepId'].some(key => typeof failure[key] !== 'string')
        || typeof failure.retryable !== 'boolean') {
      throw new NodeLocalDiagnostic('MALFORMED_FAILURE');
    }
    if (!['TRANSIENT', 'BUSINESS_REJECTION', 'UNCLASSIFIED'].includes(failure.code)) {
      throw new NodeLocalDiagnostic('UNKNOWN_FAILURE_CODE');
    }
    if (failure.owner !== 'LOCAL_SHELL' || failure.stepId !== stepId
        || failure.retryable !== (failure.code === 'TRANSIENT')
        || (failure.code === 'UNCLASSIFIED' && failure.message !== unclassified)) {
      throw new NodeLocalDiagnostic('FAILURE_BINDING_MISMATCH');
    }
    return Object.freeze(failure);
  }
}

function bindValue(value) {
  const record = snapshot(value, 'INVALID_VALUE');
  requireKeys(record, ['value'], 'INVALID_VALUE');
  const number = record.value;
  if (!Number.isInteger(number) || Object.is(number, -0) || number < -2147483648 || number > 2147483647) {
    throw new NodeLocalDiagnostic('INVALID_VALUE');
  }
  return Object.freeze(record);
}

function bindResult(value) {
  const outcome = snapshot(value, 'INVALID_VALUE');
  if (outcome.kind === 'EMPTY' && Object.keys(outcome).length === 1) {
    throw new NodeLocalDiagnostic('UNSUPPORTED_EMPTY');
  }
  requireKeys(outcome, ['kind', 'value'], 'INVALID_VALUE');
  if (outcome.kind !== 'VALUE') throw new NodeLocalDiagnostic('INVALID_VALUE');
  return bindValue(outcome.value);
}

function bindContext(value) {
  const context = snapshot(value, 'INVALID_CONTEXT');
  requireKeys(context, ['executionId', 'traceId'], 'INVALID_CONTEXT');
  if (Object.values(context).some(entry => typeof entry !== 'string' || !/[^ \t\r\n]/.test(entry))) {
    throw new NodeLocalDiagnostic('INVALID_CONTEXT');
  }
  return Object.freeze(context);
}

function watchCancellation(signal, acceptCancel) {
  if (signal === undefined) return;
  try {
    if (readAborted.call(signal)) {
      acceptCancel();
      return;
    }
    // Follow native abort state through a private signal. An arbitrary event
    // dispatched on the caller's EventTarget cannot consume this listener.
    return addAbortListener(AbortSignal.any([signal]), acceptCancel);
  } catch {
    throw new NodeLocalDiagnostic('INVALID_SIGNAL', 'expected a native AbortSignal');
  }
}

async function run({pipelineId, steps}, observe, input, callerContext, signal) {
  let terminal;
  let context;
  let cancellation;
  const finish = (record, finalReturn) => {
    if (terminal === undefined) {
      terminal = Object.freeze(record);
      // Commit a validated final return before observations can trigger cancellation.
      if (finalReturn !== undefined) observe(finalReturn);
      observe({kind: 'TERMINAL', pipelineId, context, record: terminal});
    }
    return terminal;
  };

  try {
    let value = bindValue(input);
    context = bindContext(callerContext);
    cancellation = watchCancellation(signal, () => finish({kind: 'CANCELLED'}));

    let finalReturn;
    for (const step of steps) {
      const {id: stepId, inputType, outputType, retryLimit, invoke} = step;
      let attempt = 0;
      const event = kind => ({kind, pipelineId, stepId, inputType, outputType, attempt, context});
      for (;;) {
        if (terminal !== undefined) return terminal;
        attempt += 1;
        observe(event('ATTEMPT_STARTED'));
        if (terminal !== undefined) return terminal;

        let returned;
        try {
          const result = invoke(value, context);
          // Await native promises, but do not execute a 'then' property on a malformed carrier.
          // Yield at either kind of return before the shell accepts the next event.
          if (isPromise(result)) {
            returned = await result;
          } else {
            await undefined;
            returned = result;
          }
        } catch (error) {
          if (terminal !== undefined) return terminal;
          const failure = StructuredFailure.bind(error, stepId);
          if (terminal !== undefined) return terminal;
          if (!failure.retryable || attempt > retryLimit) return finish({kind: 'FAILED', failure});
          observe(event('RETRY_SCHEDULED'));
          continue;
        }

        // A started handler is allowed to settle after cancellation. Ignore its result or
        // failure, and never dispatch a subsequent handler or retry for that invocation.
        if (terminal !== undefined) return terminal;
        value = bindResult(returned);
        if (terminal !== undefined) return terminal;
        if (step === steps.at(-1)) finalReturn = event('RETURNED');
        else observe(event('RETURNED'));
        break;
      }
    }
    return finish({kind: 'COMPLETED', outcome: Object.freeze({kind: 'VALUE', value})}, finalReturn);
  } catch (error) {
    if (error instanceof NodeLocalDiagnostic) return finish({kind: 'REJECTED', diagnostic: error.code});
    throw error;
  } finally {
    cancellation?.[Symbol.dispose]();
  }
}

/** Construct the Node LOCAL runtime for a compiler-generated createApplication call. */
export function createNodeLocalRuntime(options = {}) {
  const settings = snapshot(options, 'UNSUPPORTED_RUNTIME_OPTIONS', 'only onObservation is supported');
  const {onObservation} = settings;
  if (Object.keys(settings).some(key => key !== 'onObservation')
      || (onObservation !== undefined && typeof onObservation !== 'function')) {
    throw new NodeLocalDiagnostic('UNSUPPORTED_RUNTIME_OPTIONS', 'only onObservation is supported');
  }

  const observe = onObservation === undefined ? () => {} : event => {
    try {
      // Observations are synchronous. A callback's failure or returned promise has no
      // authority over application settlement and is never awaited by the runtime.
      const result = onObservation(Object.freeze(event));
      if (isPromise(result)) void result.catch(() => {});
    } catch {
      // Diagnostic observers cannot make a business invocation fail.
    }
  };
  return Object.freeze({
    load(binding) {
      const admission = admitBinding(binding);
      return Object.freeze({
        run: (input, context, signal) => run(admission, observe, input, context, signal),
      });
    },
  });
}
