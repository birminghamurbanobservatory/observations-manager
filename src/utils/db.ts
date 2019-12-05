//-------------------------------------------------
// Dependencies
//-------------------------------------------------
const mongoose = require('mongoose'); // .on doesn't work if I load using import
import * as logger from 'node-logger';

// plug-in bluebird promise library
mongoose.Promise = require('bluebird');

// Log successful connections and connection errors.
const db = mongoose.connection;
db.on('open', () => {
  logger.info('Succesfully connected to MongoDB database');
});
db.on('error', (err) => {
  logger.error('DB Connection Error', err);
});


//-------------------------------------------------
// Connect
//-------------------------------------------------
// Docs: https://mongoosejs.com/docs/connections.html
export function connectDb(uri): Promise<void> {
  // Resolves when the database is ready to use. However, because mongoose is clever enough to buffer model function calls internally you can start using you models immediately without having to wait for this to resolve. N.B. mongoose.connect() resolves to undefined.
  // This promise will reject if there was an initial connection error.
  // The options here help surpress some deprecation warnings on startup.
  return mongoose.connect(uri, {
    useNewUrlParser: true, 
    useCreateIndex: true, 
    useFindAndModify: false,
    useUnifiedTopology: true
  });
}


//-------------------------------------------------
// Disconnect
//-------------------------------------------------
export function disconnectDb(): Promise<void> {
  return mongoose.disconnect()
  .then(() => {
    return;
  });
}