import {areGeometriesTheSame} from './compare-geojson';


describe('Testing of areGeometriesTheSame function', () => {


  test('Comparison gives true for ever so slightly different precisions', () => {
    
    const geometry1 = {
      coordinates: [-1.95961974364944, 52.532666768472],
      type: 'Point'
    };

    const geometry2 = {
      coordinates: [-1.9596197436494434, 52.532666768472005],
      type: 'Point'
    };

    expect(areGeometriesTheSame(geometry1, geometry2)).toBe(true);

  });



  test('Comparison gives false for significantly different locations', () => {
    
    const geometry1 = {
      coordinates: [-1.9597, 52.53270],
      type: 'Point'
    };

    const geometry2 = {
      coordinates: [-1.9596, 52.53266],
      type: 'Point'
    };

    expect(areGeometriesTheSame(geometry1, geometry2)).toBe(false);

  });

});