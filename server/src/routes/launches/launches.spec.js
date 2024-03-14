const request = require('supertest');
const app = require('../../app');

describe('Test GET /launches', () => {
    test('It should respond with 200 sucess', async () => {
        const response = await request(app).get('/launches');
        expect(response).toBe(200);
    });
});

describe('Test POST /launches', () => {
    test('It should respond with 200 success', () => {
    });

    test('It should catch missing required properties', () => {});
    test('It should catch invalid dates', () => {});
});