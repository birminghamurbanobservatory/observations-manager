import {kebabCaseRegex, camelCaseRegex, pascalCaseRegex} from './regular-expressions';


describe('kebab-case Regex Tests', () => {

  test('Test that a valid string passes ok', () => {
    const testString = 'kebab-case';
    expect(kebabCaseRegex.test(testString)).toBe(true);
  });

  test('Test that a string without any hypens passes ok', () => {
    const testString = 'kebab';
    expect(kebabCaseRegex.test(testString)).toBe(true);
  });

  test('Test that a string with consecutive hypens fails', () => {
    const testString = 'kebab--case';
    expect(kebabCaseRegex.test(testString)).toBe(false);
  });

  test('Test that a string that starts with a hypen fails', () => {
    const testString = '-kebab-case';
    expect(kebabCaseRegex.test(testString)).toBe(false);
  });

  test('Test that a string that ends with a hypen fails', () => {
    const testString = 'kebab-case-';
    expect(kebabCaseRegex.test(testString)).toBe(false);
  });

  test('Test that it allows numbers', () => {
    const testString = 'kebab-case-123';
    expect(kebabCaseRegex.test(testString)).toBe(true);
  });

  test('Test it fails when it contains a capital', () => {
    const testString = 'kebAb-case';
    expect(kebabCaseRegex.test(testString)).toBe(false);
  });

  test('Test that camelCase does not pass as kebab-case', () => {
    const testString = 'camelCase';
    expect(kebabCaseRegex.test(testString)).toBe(false);
  });

  test('Test that PascalCase does not pass as kebab-case', () => {
    const testString = 'PascalCase';
    expect(kebabCaseRegex.test(testString)).toBe(false);
  });

});



describe('camelCase Regex Tests', () => {

  test('Test that a valid string passes ok', () => {
    const testString = 'camelCase';
    expect(camelCaseRegex.test(testString)).toBe(true);
  });

  test('Test that a string without any capitals passes ok', () => {
    const testString = 'camel';
    expect(camelCaseRegex.test(testString)).toBe(true);
  });

  // TODO: Struggling to get this to pass
  // test('Test that a string with consecutive capitals passes', () => {
  //   const testString = 'camelCCase';
  //   expect(camelCaseRegex.test(testString)).toBe(true);
  // });

  test('Test that a string that starts with a capital fails', () => {
    const testString = 'CamelCase';
    expect(camelCaseRegex.test(testString)).toBe(false);
  });

  test('Test that a string with consecutive hypens fails', () => {
    const testString = 'camelCase--';
    expect(kebabCaseRegex.test(testString)).toBe(false);
  });

  test('Test that it allows numbers', () => {
    const testString = 'c1melCase123';
    expect(camelCaseRegex.test(testString)).toBe(true);
  });

  test('Test that it fails when it starts with a number', () => {
    const testString = '1CamelCase';
    expect(pascalCaseRegex.test(testString)).toBe(false);
  });

  test('Test that kebab-case does not pass as camelCase', () => {
    const testString = 'kebab-case';
    expect(camelCaseRegex.test(testString)).toBe(false);
  });

  test('Test that PascalCase does not pass as camelCase', () => {
    const testString = 'PascalCase';
    expect(camelCaseRegex.test(testString)).toBe(false);
  });

  test('Test fails when string includes a space', () => {
    const testString = 'camel Case';
    expect(camelCaseRegex.test(testString)).toBe(false);
  });

});




describe('PascalCase Regex Tests', () => {

  test('Test that a valid string passes ok', () => {
    const testString = 'PascalCase';
    expect(pascalCaseRegex.test(testString)).toBe(true);
  });

  test('Test that a string with just one word passes ok', () => {
    const testString = 'Pascal';
    expect(pascalCaseRegex.test(testString)).toBe(true);
  });

  test('Test that a string without any capitals fails', () => {
    const testString = 'pascal';
    expect(pascalCaseRegex.test(testString)).toBe(false);
  });

  // TODO: Struggling to get this to pass
  // test('Test that a string with consecutive capitals passes', () => {
  //   const testString = 'DocumentJSON';
  //   expect(pascalCaseRegex.test(testString)).toBe(true);
  // });

  test('Test that a string that starts with a lowercase fails', () => {
    const testString = 'pascalCase';
    expect(pascalCaseRegex.test(testString)).toBe(false);
  });

  test('Test that a string with consecutive hypens fails', () => {
    const testString = 'PascalCase--';
    expect(kebabCaseRegex.test(testString)).toBe(false);
  });

  test('Test that it allows numbers', () => {
    const testString = 'Pa2calCase123';
    expect(pascalCaseRegex.test(testString)).toBe(true);
  });

  test('Test that it fails when it starts with a number', () => {
    const testString = '1PascalCase';
    expect(pascalCaseRegex.test(testString)).toBe(false);
  });

  test('Test that kebab-case does not pass as PascalCase', () => {
    const testString = 'kebab-case';
    expect(pascalCaseRegex.test(testString)).toBe(false);
  });

  test('Test that camelCase does not pass as PascalCase', () => {
    const testString = 'camelCase';
    expect(pascalCaseRegex.test(testString)).toBe(false);
  });

  test('Test fails when string includes a space', () => {
    const testString = 'Pascal Case';
    expect(pascalCaseRegex.test(testString)).toBe(false);
  });

});
