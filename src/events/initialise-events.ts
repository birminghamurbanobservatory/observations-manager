import * as event from 'event-stream';
import * as logger from 'node-logger';
import {withCorrelationId, getCorrelationId} from '../utils/correlator';
import {invokeAllSubscriptions} from './subscriptions';

export async function initialiseEvents(settings: {url: string; appName: string; logLevel: string}): Promise<void> {

  logger.debug('Initalising events stream');

  if (logIt('error', settings.logLevel)) {
    event.logsEmitter.on('error', (msg): void => {
      logger.error(msg);
    });
  }
  if (logIt('warn', settings.logLevel)) {
    event.logsEmitter.on('warn', (msg): void => {
      logger.warn(msg);
    });
  }
  if (logIt('info', settings.logLevel)) {
    event.logsEmitter.on('info', (msg): void => {
      logger.info(msg);
    });
  }
  if (logIt('debug', settings.logLevel)) {
    event.logsEmitter.on('debug', (msg): void => {
      logger.debug(msg);
    });
  }

  try {
    await event.init({
      url: settings.url,
      appName: settings.appName,
      withCorrelationId,
      getCorrelationId
    });
  } catch (err) {
    logger.error(`Failed to initialise event-stream. Reason: ${err.message}`);
  }

  function logIt(level, configSetting): boolean {
    const levels = ['debug', 'info', 'warn', 'error'];
    return levels.indexOf(level) >= levels.indexOf(configSetting);
  }  

  // Add the subscriptions even if the init failed (e.g. because RabbitMQ wasn't turned on yet), this ensures the subscriptions get added to the list and will be automatically re-established if the connection returns.
  await invokeAllSubscriptions();

}