import {config} from '../config';
import * as logger from 'node-logger';
import {connectDb, disconnectDb} from '../utils/db';
import * as MongodbMemoryServer from 'mongodb-memory-server';
import * as observationController from '../components/observation/observation.controller';
import * as timeseriesService from '../components/timeseries/timeseries.service';
import * as observationService from '../components/observation/observation.service';
import * as check from 'check-types';
import ObsBucket from '../components/observation/obs-bucket.model';
import Timeseries from '../components/timeseries/timeseries.model';
import {cloneDeep} from 'lodash';

describe('Test that observations are being saved correctly', () => {

  let mongoServer;

  beforeAll(() => {
    // Configure the logger
    logger.configure(config.logger);
  });

  beforeEach(() => {
    // Create fresh database
    mongoServer = new MongodbMemoryServer.MongoMemoryServer();
    return mongoServer.getConnectionString()
    .then((url) => {
      return connectDb(url);
    });    
  });

  afterEach(() => {
    // Disconnect from, then stop, database.
    return disconnectDb()
    .then(() => {
      mongoServer.stop();
      return;
    });
  });  


  test('Check we can get an observation by its ID', async () => {

    expect.assertions(1);

    // First create an observation
    const obs = {
      madeBySensor: 'sensor-456',
      inDeployments: ['deployment-3'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      hasResult: {
        value: 12.3
      },
      resultTime: '2019-12-05T18:12:06.531Z'
    };

    const createdObs = await observationController.createObservation(obs);

    // Now get it using its id
    const foundObs = await observationController.getObservation(createdObs.id);
    const expected: any = cloneDeep(obs);
    expected.id = foundObs.id;
    expected.resultTime = new Date(obs.resultTime);
    expect(foundObs).toEqual(expected);

  });




});