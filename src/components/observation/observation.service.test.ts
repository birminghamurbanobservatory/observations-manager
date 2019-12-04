import {extractTimeseriesPropsFromObservation, buildObservation, getDateAsDay, extractResultFromObservation} from './observation.service';


describe('Testing of extractTimeseriesPropsFromObservation function', () => {

  test('Should correctly extract the timeseries props from an observation', () => {
    const observation = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: '12'
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp',
    };
    const expected = {
      madeBySensor: 'sensor-123',
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp'     
    };
    expect(extractTimeseriesPropsFromObservation(observation)).toEqual(expected);
  });

});


describe('Testing of extractTimeseriesPropsFromObservation function', () => {

  test('Should correctly extract the timeseries props from a already full observation', () => {
    const observation = {
      madeBySensor: 'sensor-123',
      hasResult: {
        value: '12'
      },
      resultTime: '2019-12-04T17:26:23.205Z',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp', 
      usedProcedures: ['point-sample']
    };
    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp', 
      usedProcedures: ['point-sample']      
    };
    expect(extractTimeseriesPropsFromObservation(observation)).toEqual(expected);
  });

});


describe('Testing of buildObservation function', () => {

  test('Builds observation as expected', () => {
    
    const result = {
      value: 22.3,
      resultTime: new Date('2019-12-04T17:26:23.205Z'),
      flags: ['persistence']
    };

    const timeseries = {
      id: '5de7ec4ce9d92c08dbc5db9d',
      startDate: new Date('2019-12-04T13:22:23.133Z'),
      endDate: new Date('2019-12-04T17:26:23.205Z'),
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp', 
      usedProcedures: ['point-sample']          
    };

    const expected = {
      id: '5de7ec4ce9d92c08dbc5db9d-2019-12-04T17:26:23.205Z',
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      hasFeatureOfInterest: 'weather',
      observedProperty: 'air-temp', 
      usedProcedures: ['point-sample'],
      resultTime: new Date('2019-12-04T17:26:23.205Z'),        
      hasResult: {
        value: 22.3,
        flags: ['persistence']
      } 
    };

    expect(buildObservation(result, timeseries)).toEqual(expected);

  });


  describe('Testing getDateAsDay function', () => {
  
    test('', () => {
      const date = new Date('2019-12-04T17:26:23.205Z');
      const expected = new Date('2019-12-04');
      expect(getDateAsDay(date)).toEqual(expected);
    });
  
  });


  describe('Testing extractResultFromObservation function', () => {
  
    test('Should correctly extra the result from an observation (without any flags)', () => {

      const observation = {
        madeBySensor: 'sensor-123',
        hasResult: {
          value: 12.2
        },
        resultTime: new Date('2019-12-04T17:26:23.205Z'),
        hasFeatureOfInterest: 'weather',
        observedProperty: 'air-temp',
      };

      const expectedResult = {
        value: 12.2,
        resultTime: new Date('2019-12-04T17:26:23.205Z')
      };

      expect(extractResultFromObservation(observation)).toEqual(expectedResult);

    });


    test('Should correctly extra the result from an observation (with flags)', () => {

      const observation = {
        madeBySensor: 'sensor-123',
        hasResult: {
          value: 12.2,
          flags: ['persistence']
        },
        resultTime: new Date('2019-12-04T17:26:23.205Z'),
        hasFeatureOfInterest: 'weather',
        observedProperty: 'air-temp',
      };

      const expectedResult = {
        value: 12.2,
        resultTime: new Date('2019-12-04T17:26:23.205Z'),
        flags: ['persistence']
      };

      expect(extractResultFromObservation(observation)).toEqual(expectedResult);

    });
  
  });

});