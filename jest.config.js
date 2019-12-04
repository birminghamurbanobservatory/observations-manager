module.exports = {
  roots: [
    '<rootDir>/src'
  ],
  transform: {
    '^.+\\.tsx?$': 'ts-jest'
  },
  testEnvironment: 'node'
};


// Explanation:
// - It is recommended to have all TypeScript files in a src folder in your project. We assume this is true and specify this using the roots option.
// - The transform config just tells jest to use ts-jest for ts / tsx files.
// Using testEnvironment: 'node' is recommended by mongoose (https://mongoosejs.com/docs/jest.html).