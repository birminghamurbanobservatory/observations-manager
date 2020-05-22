import {extractTimeseriesPropsFromObservation, generateObservationId, deconstructObservationId, observationAppToClient, observationClientToApp, observationDbToApp, buildObservationDb} from './observation.service';
import * as check from 'check-types';
import {InvalidObservationId} from './errors/InvalidObservationId';


describe('Testing of extractTimeseriesPropsFromObservation function', () => {

  test('Should correctly extract the timeseries props from an observation', () => {
    const observation = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: '12'
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      disciplines: ['meteorology']
    };
    const expected = {
      madeBySensor: 'sensor-123',
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      disciplines: ['meteorology']     
    };
    expect(extractTimeseriesPropsFromObservation(observation)).toEqual(expected);
  });


  test('Should correctly extract the timeseries props from an already full observation', () => {
    const observation = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: '12',
        unit: 'degree-celsius'
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'air-temperature',
      aggregation: 'instant',
      disciplines: ['meteorology'],
      usedProcedures: ['point-sample']
    };
    const expected = {
      madeBySensor: 'sensor-123',
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'air-temperature', 
      aggregation: 'instant',
      unit: 'degree-celsius',
      disciplines: ['meteorology'],
      usedProcedures: ['point-sample']      
    };
    expect(extractTimeseriesPropsFromObservation(observation)).toEqual(expected);
  });

});


describe('Testing of generateObservationId function', () => {

  test('Should encode and decode and get the same component values back (with input resultTime as string)', () => {
    const timeseriesId = 543;
    const resultTime = '2019-12-06T14:57:18.838Z';
    const id = generateObservationId(timeseriesId, resultTime);
    expect(check.nonEmptyString(id)).toBe(true);
    const decoded = deconstructObservationId(id);
    const expected = {
      timeseriesId: 543,
      resultTime: new Date('2019-12-06T14:57:18.838Z')
    };
    expect(decoded).toEqual(expected);
  });


  test('Should encode and decode and get the same component values back (with input resultTime as date)', () => {
    const timeseriesId = 543;
    const resultTime = new Date('2019-12-06T14:57:18.838Z');
    const id = generateObservationId(timeseriesId, resultTime);
    expect(check.nonEmptyString(id)).toBe(true);
    const decoded = deconstructObservationId(id);
    const expected = {
      timeseriesId: 543,
      resultTime: new Date('2019-12-06T14:57:18.838Z')
    };
    expect(decoded).toEqual(expected);
  });  


  test('Should encode and decode and get the same component values back when a location is included', () => {
    const timeseriesId = 543;
    const resultTime = new Date('2019-12-06T14:57:18.838Z');
    const locationId = 23;
    const id = generateObservationId(timeseriesId, resultTime, locationId);
    expect(check.nonEmptyString(id)).toBe(true);
    const decoded = deconstructObservationId(id);
    const expected = {
      timeseriesId: 543,
      resultTime: new Date('2019-12-06T14:57:18.838Z'),
      locationId: 23
    };
    expect(decoded).toEqual(expected);
  });


  test('Should throw an error if you give it a really random string to try and decode', () => {
    const clientId = 'fhyewgrYEHEYHUUEIDJJE333333';
    expect(() => {
      deconstructObservationId(clientId);
    }).toThrowError(InvalidObservationId);
  });  

});



describe('observationClientToApp function tests', () => {

  test('Can convert a fairly basic observation correctly.', () => {

    const observationClient = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: 12
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'air-temperature', 
      aggregation: 'instant',
      usedProcedures: ['point-sample']
    };

    const expected = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: 12
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'air-temperature', 
      aggregation: 'instant',
      usedProcedures: ['point-sample']
    };

    const observationApp = observationClientToApp(observationClient);
    expect(observationApp).toEqual(expected);

  });

  test('Can convert an observation that has a phenomenonTime object correctly.', () => {

    const observationClient = {
      madeBySensor: 'rain-gauge-123',
      hasResult: {
        value: 0.3
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      phenomenonTime: {
        hasBeginning: '2019-12-04T17:16:23.205Z',
        hasEnd: '2019-12-04T17:26:23.205Z',
        duration: 600
      },
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'precipitation-depth',
      aggregation: 'sum'
    };

    const expected = {
      madeBySensor: 'rain-gauge-123',
      hasResult: {
        value: 0.3
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      phenomenonTime: {
        hasBeginning: new Date('2019-12-04T17:16:23.205Z'),
        hasEnd: new Date('2019-12-04T17:26:23.205Z'),
        duration: 600
      },
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'precipitation-depth',
      aggregation: 'sum',
    };

    const observationApp = observationClientToApp(observationClient);
    expect(observationApp).toEqual(expected);

  });

});



describe('observationAppToClient function tests', () => {

  test('Can convert a fairly basic observation correctly.', () => {

    const observationApp = {
      id: 12424,
      clientId: '151-12-2019-12-04T17:26:23.205Z',
      timeseriesId: 5223,
      madeBySensor: 'sensor-123',
      hasResult: {
        value: 12
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'air-temperature', 
      aggregation: 'instant',
      usedProcedures: ['point-sample']
    };

    const expected: any = {
      id: '151-12-2019-12-04T17:26:23.205Z',
      madeBySensor: 'sensor-123',
      hasResult: {
        value: 12
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'air-temperature', 
      aggregation: 'instant',
      usedProcedures: ['point-sample']
    };

    const observationClient = observationAppToClient(observationApp);
    // We can't easily predict the id due to the hashing, so add it here. The id construction is tested elsewhere anyway.
    expect(check.nonEmptyString(observationClient.timeseriesId)).toBe(true);
    expected.timeseriesId = observationClient.timeseriesId;

    expect(observationClient).toEqual(expected);

  });


  test('Can convert an observation that has a phenomenonTime object correctly.', () => {

    const observationApp = {
      id: 12424,
      clientId: '111-9-2019-12-04T17:26:23.205Z',
      madeBySensor: 'rain-gauge-123',
      hasResult: {
        value: 0.3
      },
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      phenomenonTime: {
        hasBeginning: new Date('2019-12-04T17:16:23.205Z'),
        hasEnd: new Date('2019-12-04T17:26:23.205Z'),
        duration: 600
      },
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'precipitation-depth',
      aggregation: 'sum',
    };

    const expected = {
      id: '111-9-2019-12-04T17:26:23.205Z',
      madeBySensor: 'rain-gauge-123',
      hasResult: {
        value: 0.3
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      phenomenonTime: {
        hasBeginning: '2019-12-04T17:16:23.205Z',
        hasEnd: '2019-12-04T17:26:23.205Z',
        duration: 600
      },
      hasDeployment: 'deployment-1',
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'precipitation-depth',
      aggregation: 'sum'
    };

    const observationClient = observationAppToClient(observationApp);
    expect(observationClient).toEqual(expected);

  });  

});


describe('observationDbToApp function tests', () => {

  test('Converts db response correctly', () => {
    
    const observationDb = {
      id: '223253',
      timeseries_id: 54,
      location_id: 33,
      result_time: '2019-12-04T17:26:23.205Z',
      has_beginning: '2019-12-04T17:16:23.205Z',
      has_end: '2019-12-04T17:26:23.205Z',
      duration: 600,
      value_number: '0.3',
      value_boolean: null,
      value_text: null,
      value_json: null,
      flags: null,
      made_by_sensor: 'rain-gauge-123',
      hasDeployment: 'deployment-1',
      hosted_by_path: 'bob-back-garden.platform-1',
      has_feature_of_interest: 'earth-atmosphere',
      observed_property: 'precipitation-depth',
      aggregation: 'sum',
      used_procedures: ['tip-sum'],
      location_client_id: '146380a6-0614-48ce-a0ae-a1bf935f015c',
      location_geojson: {type: 'Point', coordinates: [-1.9, 52.9]},
      location_valid_at: '2019-07-05T12:43:24.621Z' 
    };

    const expected: any = {
      id: 223253,
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      timeseriesId: 54,
      phenomenonTime: {
        hasBeginning: new Date('2019-12-04T17:16:23.205Z'),
        hasEnd: new Date('2019-12-04T17:26:23.205Z'),
        duration: 600
      },
      hasResult: {
        value: 0.3
      },
      madeBySensor: 'rain-gauge-123',
      hasDeployment: 'deployment-1',
      hostedByPath: ['bob-back-garden', 'platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'precipitation-depth',
      aggregation: 'sum',
      usedProcedures: ['tip-sum'],
      location: {
        id: 33,
        clientId: '146380a6-0614-48ce-a0ae-a1bf935f015c',
        geometry: {type: 'Point', coordinates: [-1.9, 52.9]},
        validAt: new Date('2019-07-05T12:43:24.621Z')
      }

    };

    const observationApp = observationDbToApp(observationDb);

    // We can't easily predict the id due to the hashing, so add it here. The id construction is tested elsewhere anyway.
    expect(check.nonEmptyString(observationApp.clientId)).toBe(true);
    expected.clientId = observationApp.clientId;
    
    expect(observationApp).toEqual(expected);

  });


  test('Will omit the phenomenonTime object when its not required', () => {
    
    const observationDb = {
      id: '223253',
      timeseries_id: 54,
      location_id: 33,
      result_time: '2019-12-04T17:26:23.205Z',
      has_beginning: '2019-12-04T17:26:23.205Z', // same as result_time
      has_end: '2019-12-04T17:26:23.205Z', // same as result_time
      duration: 0, // 0 as we're simulating an Instant observation
      value_number: '0.3',
      value_boolean: null,
      value_text: null,
      value_json: null,
      flags: null,
      made_by_sensor: 'rain-gauge-123',
      hasDeployment: 'deployment-1',
      hosted_by_path: 'bob-back-garden.platform-1',
      has_feature_of_interest: 'earth-atmosphere',
      observed_property: 'precipitation-depth',
      aggregation: 'sum',
      used_procedures: ['tip-sum'],
      location_client_id: '146380a6-0614-48ce-a0ae-a1bf935f015c',
      location_geojson: {type: 'Point', coordinates: [-1.9, 52.9]},
      location_valid_at: '2019-07-05T12:43:24.621Z' 
    };

    const expected: any = {
      id: 223253,
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      timeseriesId: 54,
      hasResult: {
        value: 0.3
      },
      madeBySensor: 'rain-gauge-123',
      hasDeployment: 'deployment-1',
      hostedByPath: ['bob-back-garden', 'platform-1'],
      hasFeatureOfInterest: 'earth-atmosphere',
      observedProperty: 'precipitation-depth',
      aggregation: 'sum',
      usedProcedures: ['tip-sum'],
      location: {
        id: 33,
        clientId: '146380a6-0614-48ce-a0ae-a1bf935f015c',
        geometry: {type: 'Point', coordinates: [-1.9, 52.9]},
        validAt: new Date('2019-07-05T12:43:24.621Z')
      }

    };

    const observationApp = observationDbToApp(observationDb);

    // We can't easily predict the id due to the hashing, so add it here. The id construction is tested elsewhere anyway.
    expect(check.nonEmptyString(observationApp.clientId)).toBe(true);
    expected.clientId = observationApp.clientId;
    
    expect(observationApp).toEqual(expected);

  });

});



describe('Testing buildObservationDb function', () => {

  test('Check defaults will be added for has_beginning, has_end and duration', () => {
    
    const obsCore = {
      resultTime: new Date('2019-07-05T12:43:24.621Z'),
      value: 10.1
    };

    const timeseriesId = 5;

    const expected = {
      timeseries: 5,
      result_time: '2019-07-05T12:43:24.621Z',
      value_number: 10.1,
      has_beginning: '2019-07-05T12:43:24.621Z',
      has_end: '2019-07-05T12:43:24.621Z',
      duration: 0
    };

    const observationDb = buildObservationDb(obsCore, timeseriesId);
    expect(observationDb).toEqual(expected);

  });

});