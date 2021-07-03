//  k6 run --vus 100 --duration 10s load.js
import http from 'k6/http';
import { sleep } from 'k6';

export default function () {
  var url = 'http://localhost:3030/bb';
  var payload = JSON.stringify({
    content: 'aaa',
    h: 'bbb',
  });

  var params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  http.post(url, payload, params);
}