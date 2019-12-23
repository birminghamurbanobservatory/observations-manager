import {whereToMongoFind} from './where-to-mongo-find';


describe('Conversion of where object to MongoDB/Mongoose find object', () => {

  test('Converts and empty object', () => {
    const where = {};
    const expected = {};
    expect(whereToMongoFind(where)).toEqual(expected);
  });


  test('Converts a substantial where object', () => {
    const where = {
      inDeployment: 'deployment-1',
      madeBySensor: {
        in: ['sensor-1', 'sensor-2']
      },
      isHostedBy: {
        exists: false
      },
      createdAt: {
        gte: '2019-11-29'
      }
    };
    const expected = {
      inDeployment: 'deployment-1',
      isHostedBy: {$exists: false},
      createdAt: {$gte: '2019-11-29'},
      madeBySensor: {$in: ['sensor-1', 'sensor-2']}
    };
    expect(whereToMongoFind(where)).toEqual(expected);
  });


  test('Can handle a property that has several operators in its object', () => {
    const where = {
      createdAt: {
        gte: '2019-11-29',
        lt: '2019-12-04'
      }
    };
    const expected = {
      createdAt: {$gte: '2019-11-29', $lt: '2019-12-04'}
    };
    expect(whereToMongoFind(where)).toEqual(expected);
  });



});