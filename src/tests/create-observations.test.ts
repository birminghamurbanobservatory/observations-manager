import {config} from '../config';
import * as logger from 'node-logger';
import {connectDb, disconnectDb} from '../db/mongodb-service';
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

    expect.assertions(12);

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

    const createdObs2 = await observationController.createObservation(obs2);

    const allTimeseriesB = await Timeseries.find({}).exec();
    expect(allTimeseriesB.length).toBe(1);
    const timeseriesB = timeseriesService.timeseriesDbToApp(allTimeseriesB[0]);
    expect(timeseriesB.id).toBe(timeseriesA.id);
    const obsBucketsB = await ObsBucket.find({}).exec();
    expect(obsBucketsB.length).toBe(1);
    const obsBucketB = observationService.obsBucketDbToApp(obsBucketsB[0]);
    expect(check.nonEmptyString(obsBucketB.id)).toBe(true);

    expect(createdObs2.id).toBe(`${timeseriesB.id}-${obs2.resultTime}`);

    expect(timeseriesB).toEqual({
      id: timeseriesB.id,
      startDate: new Date(obs1.resultTime),
      endDate: new Date(obs2.resultTime),
      madeBySensor: observationBase.madeBySensor,
      inDeployments: observationBase.inDeployments,
      hostedByPath: observationBase.hostedByPath,
      observedProperty: observationBase.observedProperty,
      hasFeatureOfInterest: observationBase.hasFeatureOfInterest,
      usedProcedures: observationBase.usedProcedures
    });

    expect(obsBucketB).toEqual({
      id: obsBucketB.id,
      timeseries: timeseriesB.id,
      startDate: new Date(obs1.resultTime),
      endDate: new Date(obs2.resultTime),
      nResults: 2,
      day: new Date('2019-12-05'),
      results: [
        {
          value: obs1.hasResult.value,
          resultTime: new Date(obs1.resultTime)
        },
        {
          value: obs2.hasResult.value,
          resultTime: new Date(obs2.resultTime)
        }
      ]
    });

  });



  test('Try some observations that will be allocated to different timeseries.', async () => {

    expect.assertions(8);

    const obs1 = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample'],
      hasResult: {
        value: 22.2
      },
      resultTime: '2019-12-05T17:05:07.937Z'
    };

    const obs2 = {
      madeBySensor: 'sensor-456',
      inDeployments: ['deployment-3'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      hasResult: {
        value: 'low'
      },
      resultTime: '2019-12-05T18:12:06.531Z'
    };

    const createdObs1 = await observationController.createObservation(obs1);
    const createdObs2 = await observationController.createObservation(obs2);

    const allTimeseries = await Timeseries.find({}).exec();
    expect(allTimeseries.length).toBe(2);
    const timeseriesA = timeseriesService.timeseriesDbToApp(allTimeseries[0]);
    const timeseriesB = timeseriesService.timeseriesDbToApp(allTimeseries[1]);
    const obsBuckets = await ObsBucket.find({}).exec();
    expect(obsBuckets.length).toBe(2);
    const obsBucketA = observationService.obsBucketDbToApp(obsBuckets[0]);
    const obsBucketB = observationService.obsBucketDbToApp(obsBuckets[1]);

    expect(createdObs1.id).toBe(`${timeseriesA.id}-${obs1.resultTime}`);
    expect(createdObs2.id).toBe(`${timeseriesB.id}-${obs2.resultTime}`);

    expect(timeseriesA).toEqual({
      id: timeseriesA.id,
      startDate: new Date(obs1.resultTime),
      endDate: new Date(obs1.resultTime),
      madeBySensor: obs1.madeBySensor,
      inDeployments: obs1.inDeployments,
      hostedByPath: obs1.hostedByPath,
      observedProperty: obs1.observedProperty,
      hasFeatureOfInterest: obs1.hasFeatureOfInterest,
      usedProcedures: obs1.usedProcedures
    });

    expect(timeseriesB).toEqual({
      id: timeseriesB.id,
      startDate: new Date(obs2.resultTime),
      endDate: new Date(obs2.resultTime),
      madeBySensor: obs2.madeBySensor,
      inDeployments: obs2.inDeployments,
      observedProperty: obs2.observedProperty,
      hasFeatureOfInterest: obs2.hasFeatureOfInterest
    });

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

    expect(obsBucketB).toEqual({
      id: obsBucketB.id,
      timeseries: timeseriesB.id,
      startDate: new Date(obs2.resultTime),
      endDate: new Date(obs2.resultTime),
      nResults: 1,
      day: new Date('2019-12-05'),
      results: [
        {
          value: obs2.hasResult.value,
          resultTime: new Date(obs2.resultTime)
        }
      ]
    });

  });




});