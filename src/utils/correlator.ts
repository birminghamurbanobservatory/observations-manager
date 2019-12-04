import * as cls from 'cls-hooked';
import * as shortId from 'shortid';

// Based on: https://medium.com/@evgeni.kisel/add-correlation-id-in-node-js-applications-fde759eed5e3

const store = cls.createNamespace(`correlation-id-namespace`);

const CORRELATION_ID_KEY = `correlation-id`;

// executes specified function with correlation ID. If ID is missing then new ID is generated
export async function withCorrelationId(fn, id): Promise<any> {
  return store.runAndReturn((): any => {
    setCorrelationId(id);
    return fn();
  });
}

function setCorrelationId(id): void {
  store.set(CORRELATION_ID_KEY, id || shortId.generate());
  return;
}

export function getCorrelationId(): string {
  return store.get(CORRELATION_ID_KEY);
}

export const bindEmitter = store.bindEmitter.bind(store);
export const bind = store.bind.bind(store);