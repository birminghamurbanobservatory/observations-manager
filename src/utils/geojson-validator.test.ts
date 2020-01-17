import {validateGeometry} from './geojson-validator';
import {InvalidGeometry} from './InvalidGeometry';

//-------------------------------------------------
// Tests
//-------------------------------------------------
describe('Tests for validateGeometry function', () => {

  test('Validates valid point geometry', () => {
    const geometry = {type: 'Point', coordinates: [-0.124573, 51.500705]};
    expect(validateGeometry(geometry)).toBe(undefined);
  });

  test('Validates valid point geometry with elevation', () => {
    const geometry = {type: 'Point', coordinates: [-0.124573, 51.500705, 96]};
    expect(validateGeometry(geometry)).toBe(undefined);
  });

  test('Throws error when passed full GeoJSON rather than just the geometry', () => {
    const geojson = {
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [-0.124573, 51.500705, 96]
      },
      properties: {
        name: 'Big Ben'
      }
    };

    expect(() => {
      validateGeometry(geojson);
    }).toThrowError(InvalidGeometry);
  });  

  // Docs: https://tools.ietf.org/html/rfc7946#section-3.1.6
  test('Throws error when polygon does not follow right-hand rule', () => {
    const geometry = {
      type: 'Polygon',
      coordinates: [
        [
          [-1.9345432, 52.4352342], [-1.9645432, 52.4352342], [-1.9645432, 52.4452342], [-1.9345432, 52.4352342]
        ]
      ]
    };

    expect(() => {
      validateGeometry(geometry);
    }).toThrowError(InvalidGeometry);

    expect(() => {
      validateGeometry(geometry);
    }).toThrowError('right-hand');
  });

  test('Validates valid polygon geometry', () => {
    const geometry = {
      type: 'Polygon',
      coordinates: [
        [
          [-1.9345432, 52.4352342], [-1.9645432, 52.4452342], [-1.9645432, 52.4352342], [-1.9345432, 52.4352342]
        ]
      ]
    };
    expect(validateGeometry(geometry)).toBe(undefined);
  });

  test('Throw error when passed invalid geometry', () => {
    const geometry = {typeeee: 'Point', coordinate: [-0.124573, 51.500705, 96]};
    expect(() => {
      validateGeometry(geometry);
    }).toThrowError(InvalidGeometry);

    expect(() => {
      validateGeometry(geometry);
    }).toThrowError('type'); // the error message should include the word type
  });

  test('Does not allow geometrys in string format', () => {
    const geometry = '{"type": "Point", "coordinates": [-0.124573, 51.500705]}';
    expect(() => {
      validateGeometry(geometry);
    }).toThrowError(InvalidGeometry);
  });

  test('Allows coordinates with low level of precision', () => {
    const geometry = {type: 'Point', coordinates: [-1, 51]};
    expect(validateGeometry(geometry)).toBe(undefined);
  });


});


