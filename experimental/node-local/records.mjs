export class NodeLocalDiagnostic extends Error {
  constructor(code, detail) {
    super(detail === undefined ? code : `${code}: ${detail}`);
    this.name = 'NodeLocalDiagnostic';
    this.code = code;
  }
}

export function requireKeys(record, names, code, detail) {
  const keys = Object.keys(record);
  if (keys.length !== names.length || !names.every(name => Object.hasOwn(record, name))) {
    throw new NodeLocalDiagnostic(code, detail);
  }
}

// Read each own data property once, without invoking getters or carrying caller prototypes forward.
export function snapshot(value, code, detail) {
  if (value === null || typeof value !== 'object') {
    throw new NodeLocalDiagnostic(code, detail);
  }
  let descriptors;
  try {
    if (!Array.isArray(value)) descriptors = Object.getOwnPropertyDescriptors(value);
  } catch {
    throw new NodeLocalDiagnostic(code, detail);
  }
  if (!descriptors) throw new NodeLocalDiagnostic(code, detail);
  return Object.fromEntries(Reflect.ownKeys(descriptors).map(key => {
    const descriptor = descriptors[key];
    if (typeof key !== 'string' || !descriptor.enumerable || !Object.hasOwn(descriptor, 'value')) {
      throw new NodeLocalDiagnostic(code, detail);
    }
    return [key, descriptor.value];
  }));
}
