import { NodeLocalDiagnostic, requireKeys, snapshot } from './records.mjs';

const encoding = 'tpf-polyglot-int32-json/1';
const identifier = /^[A-Za-z][A-Za-z0-9_]*$/;
const envelopeKeys = ['payloadTypeId', 'payloadEncoding', 'payload'];
// Match the closed JSON shape before number conversion. Only five literal letters or
// Unicode escapes can spell the field name; at most ten decimal digits can fit an int32.
const record = /^[ \t\r\n]*\{[ \t\r\n]*("(?:[A-Za-z]|\\u[0-9A-Fa-f]{4}){5}")[ \t\r\n]*:[ \t\r\n]*(-?(?:0|[1-9][0-9]{0,9}))[ \t\r\n]*\}[ \t\r\n]*$/;

function isInt32(value) {
  return Number.isInteger(value) && value >= -2147483648 && value <= 2147483647;
}

/** Opt in using the four logical type IDs from a verified compiler projection. */
export class PolyglotInt32PayloadBinding {
  #typeIds;

  constructor(...typeIds) {
    if (typeIds.length !== 4
        || typeIds.some(id => typeof id !== 'string' || !identifier.test(id))
        || new Set(typeIds).size !== 4) {
      throw new NodeLocalDiagnostic('INVALID_PAYLOAD_CATALOG');
    }
    this.#typeIds = new Set(typeIds);
  }

  encode(typeId, value) {
    this.#requireKnownType(typeId);
    if (!isInt32(value)) throw new NodeLocalDiagnostic('INVALID_PAYLOAD_VALUE');
    return Object.freeze({
      payloadTypeId: typeId,
      payloadEncoding: encoding,
      payload: '{"value":' + value + '}',
    });
  }

  decode(expectedTypeId, envelope) {
    this.#requireKnownType(expectedTypeId);
    const fields = snapshot(envelope, 'INVALID_PAYLOAD_ENVELOPE');
    requireKeys(fields, envelopeKeys, 'INVALID_PAYLOAD_ENVELOPE');
    if (envelopeKeys.some(key => typeof fields[key] !== 'string')) {
      throw new NodeLocalDiagnostic('INVALID_PAYLOAD_ENVELOPE');
    }
    if (fields.payloadTypeId !== expectedTypeId) {
      throw new NodeLocalDiagnostic('PAYLOAD_TYPE_MISMATCH');
    }
    if (fields.payloadEncoding !== encoding) {
      throw new NodeLocalDiagnostic('PAYLOAD_ENCODING_MISMATCH');
    }
    const match = record.exec(fields.payload);
    if (!match || JSON.parse(match[1]) !== 'value') {
      throw new NodeLocalDiagnostic('INVALID_PAYLOAD_VALUE');
    }
    const value = Number(match[2]);
    if (!isInt32(value)) throw new NodeLocalDiagnostic('INVALID_PAYLOAD_VALUE');
    return value === 0 ? 0 : value;
  }

  #requireKnownType(typeId) {
    if (!this.#typeIds.has(typeId)) throw new NodeLocalDiagnostic('UNKNOWN_PAYLOAD_TYPE');
  }
}
