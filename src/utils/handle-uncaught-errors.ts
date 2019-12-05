//-------------------------------------------------
// Dependencies
//-------------------------------------------------
import * as logger from 'node-logger';

// Guide: https://www.loggly.com/blog/node-js-error-handling/


//-------------------------------------------------
// Uncaught Exception
//-------------------------------------------------
// Log the error using our custom logger before allowing the app to crash.
process.on('uncaughtException', (err): void => {
  logger.error('Uncaught exception occurred. Crashing app now.', err);
  if (err.message.includes('Unable to compile TypeScript')) {
    // This prints the typescript error out a bit nicer
    throw err;
  }
  process.exit(1);
});


//-------------------------------------------------
// Unhandled Rejection
//-------------------------------------------------
// Not using a proper .catch(…) rejection handler with Promises will cause an unhandledRejection event to be emitted. Let's log it so it can be inspected.
process.on('unhandledRejection', (reason, promise): void => {
  logger.error('Unhandled rejection occurred. Check your Promise code.', {reason, promise});
});



