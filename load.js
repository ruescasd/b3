//  k6 run load.js
import http from 'k6/http';
import { sleep } from 'k6';

export let options = {
  scenarios: {
    open_model: {
      executor: 'constant-arrival-rate',
      rate: 20000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 500,
      maxVUs: 1000
    },
  },
};

export default function () {
  var url = 'http://localhost:3030/bb';
  var payload = JSON.stringify({
    content: 'adfgdfgsdfgsdfgsdfgsdfgsdfgsdfgsdfgsdfgsdfgsdfgsfgsdfgsdfgdfgsdfgsdggsergerg',
    hash: 'bbb',
  });

  var params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  http.post(url, payload, params);
}