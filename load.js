//  k6 run load.js
import http from 'k6/http';
import { sleep } from 'k6';

export let options = {
  scenarios: {
    open_model: {
      executor: 'constant-arrival-rate',
      rate: 2000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 50,
      maxVUs: 200
    },
  },
};

export default function () {
  var url = 'http://localhost:3030/bb';
  var payload = JSON.stringify({
    content: 'adfgdfgsdfgsdfgsdfgsdfgsdfgsdfgsdfgsdfgsdfgsdfgsfgsdfgsdfgdfgsdfgsdggsergerg',
    h: 'bbb',
  });

  var params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  http.post(url, payload, params);
}