import {convertPropsToExactWhere, timeseriesAppToDb} from './timeseries.service';
import {TimeseriesApp} from './timeseries-app.class';
import {TimeseriesDb} from './timeseries-db.class';


describe('Testing of convertPropsToExactWhere function', () => {

  test('Converts a full set of props', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample']
    };

    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: ['platform-1'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample']      
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

  });

  test('Converts a missing props to exists operators', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
    };

    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-1'],
      hostedByPath: {exists: false},
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: {exists: false}     
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

  });



  test('Re-orderes the appropriate arrays', () => {
    
    const props = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-b', 'deployment-c', 'deployment-a'],
      hostedByPath: ['lamppost-12', 'beta-weather-station'],
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample', 'averaged']     
    };

    const expected = {
      madeBySensor: 'sensor-123',
      inDeployments: ['deployment-a', 'deployment-b', 'deployment-c'], // do want this re-ordered
      hostedByPath: ['lamppost-12', 'beta-weather-station'], // don't want this re-ordered, as the order means something
      observedProperty: 'air-temperature',
      hasFeatureOfInterest: 'weather',
      usedProcedures: ['point-sample', 'averaged']  // don't want this re-ordered, as the order means something
    };

    expect(convertPropsToExactWhere(props)).toEqual(expected);

    // We also want to check it doesn't mutate the input array
    expect(props.inDeployments).toEqual(['deployment-b', 'deployment-c', 'deployment-a']);

  });


  describe('timeseriesAppToDb function testing', () => {
  
    test('Converts correctly', () => {
      const timeseriesApp: TimeseriesApp = {
        firstObs: new Date('2020-01-17T10:33:21.620Z'),
        lastObs: new Date('2020-01-17T14:43:21.420Z'),
        madeBySensor: 'sensor-1',
        inDeployments: ['deployment-2', 'deployment-1'],
        hostedByPath: ['building-1', 'room-no-3', 'device'],
        hasFeatureOfInterest: 'buildings',
        observedProperty: 'air-temperature',
        usedProcedures: ['point-sample']        
      };
      const expected: TimeseriesDb = {
        first_obs: new Date('2020-01-17T10:33:21.620Z'),
        last_obs: new Date('2020-01-17T14:43:21.420Z'),
        made_by_sensor: 'sensor-1',
        in_deployments: ['deployment-1', 'deployment-2'], // N.B. now alphabetical
        hosted_by_path: 'building_1.room_no_3.device',
        has_feature_of_interest: 'buildings',
        observed_property: 'air-temperature',
        used_procedures: ['point-sample']
      };
      const timeseriesDb = timeseriesAppToDb(timeseriesApp);
      expect(timeseriesDb).toEqual(expected);
    });
  
  });


});