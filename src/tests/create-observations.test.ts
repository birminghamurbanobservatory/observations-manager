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


  test('Saves a few observations, with the same properties, correctly.', async () => {

    expect.assertions(6);

    const observationBase = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample']
    };

    const obs1Extras = {
      hasResult: {
        value: 22.2
      },
      resultTime: '2019-12-05T17:05:07.937Z'
    };

    const obs2Extras = {
      hasResult: {
        value: 22.4
      },
      resultTime: '2019-12-05T17:10:06.531Z'
    };

    const obs1 = Object.assign({}, observationBase, obs1Extras);
    const obs2 = Object.assign({}, observationBase, obs2Extras);

    const createdObs1 = await observationController.createObservation(obs1);
    expect(check.nonEmptyString(createdObs1.id)).toBe(true);

    const allTimeseriesA = await Timeseries.find({}).exec();
    expect(allTimeseriesA.length).toBe(1);
    const timeseriesA = timeseriesService.timeseriesDbToApp(allTimeseriesA[0]);
    const obsBucketsA = await ObsBucket.find({}).exec();
    expect(obsBucketsA.length).toBe(1);
    const obsBucketA = observationService.obsBucketDbToApp(obsBucketsA[0]);
    expect(check.nonEmptyString(obsBucketA.id)).toBe(true);

    expect(createdObs1.id).toBe(`${timeseriesA.id}-${obs1.resultTime}`);

    expect(obsBucketA).toEqual({
      id: obsBucketA.id,
      timeseries: timeseriesA.id,
      startDate: new Date(obs1.resultTime),
      endDate: new Date(obs1.resultTime),
      nResults: 1,
      day: new Date('2019-12-05'),
      results: [
        {
          value: obs1.hasResult.value,
          resultTime: new Date(obs1.resultTime)
        }
      ]
    });

    // TODO: Need to create the second observation and check everything updates ok


  });



});