/*!
 *
 *  Web Starter Kit
 *  Copyright 2015 Google Inc. All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License
 *
 */
/* eslint-env browser */
(function() {
  'use strict';
  // Check to make sure service workers are supported in the current browser,
  // and that the current page is accessed from a secure origin. Using a
  // service worker from an insecure origin will trigger JS console errors. See
  // http://www.chromium.org/Home/chromium-security/prefer-secure-origins-for-powerful-new-features
  var isLocalhost = Boolean(window.location.hostname === 'localhost' ||
      // [::1] is the IPv6 localhost address.
      window.location.hostname === '[::1]' ||
      // 127.0.0.1/8 is considered localhost for IPv4.
      window.location.hostname.match(
        /^127(?:\.(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}$/
      )
    );

  if ('serviceWorker' in navigator &&
      (window.location.protocol === 'https:' || isLocalhost)) {
    navigator.serviceWorker.register('service-worker.js')
    .then(function(registration) {
      // updatefound is fired if service-worker.js changes.
      registration.onupdatefound = function() {
        // updatefound is also fired the very first time the SW is installed,
        // and there's no need to prompt for a reload at that point.
        // So check here to see if the page is already controlled,
        // i.e. whether there's an existing service worker.
        if (navigator.serviceWorker.controller) {
          // The updatefound event implies that registration.installing is set:
          // https://slightlyoff.github.io/ServiceWorker/spec/service_worker/index.html#service-worker-container-updatefound-event
          var installingWorker = registration.installing;

          installingWorker.onstatechange = function() {
            switch (installingWorker.state) {
              case 'installed':
                // At this point, the old content will have been purged and the
                // fresh content will have been added to the cache.
                // It's the perfect time to display a "New content is
                // available; please refresh." message in the page's interface.
                break;

              case 'redundant':
                throw new Error('The installing ' +
                                'service worker became redundant.');

              default:
                // Ignore
            }
          };
        }
      };
    }).catch(function(e) {
      console.error('Error during service worker registration:', e);
    });
  }
  /* Charts */
  document.getElementById('task1').addEventListener('click', getInfluence());
  document.getElementById('task2').addEventListener('click', getTrends());
  document.getElementById('task3').addEventListener('click', getUsage());
  document.getElementById('task4').addEventListener('click', getPopular());
  /* End Charts */

})();

function getPopular() {
  httpGetAsync('http://localhost:8080/query/popular?count=5', function(response) {
    var newSeries = [];
    var newLabels = [];
    response.forEach(function (current) {
      newLabels.push(current._1);
      newSeries.push(current._2);
    });
    new Chartist.Pie('#donutChart', {
      labels: newLabels,
      series: newSeries
    }, {
      donut: true,
      donutWidth: 90,
      donutSolid: true,
      startAngle: 270,
      showLabel: true
    });
  });
}

function getUsage() {
  httpGetAsync('http://localhost:8080/query/usage', function(response) {
    var newLabels = [];
    var newSeries = [];
    var one = [];
    var two = [];
    response.forEach(function(current) {
      newLabels.push(current.name);
      one.push(current.count1);
      two.push(current.count2);
    });
    newSeries.push(one);
    newSeries.push(two);
    new Chartist.Bar('#barChart', {
      labels: newLabels,
      series: newSeries
    }, {
      stackBars: true,
      axisY: {
        labelInterpolationFnc: function(value) {
          return value;
        }
      }
    }).on('draw', function(data) {
      if(data.type === 'bar') {
        data.element.attr({
          style: 'stroke-width: 30px'
        });
      }
    });
  });
}

function getTrends() {
  httpGetAsync('http://localhost:8080/query/trends', function(response) {
    var timeSeries = [];
    response.forEach(function(currentValue) {
      var newData = [];
      currentValue.data.forEach(function(cv) {
        newData.push({
          x: new Date(cv._1),
          y: parseInt(cv._2, 10)
        });
      });
      timeSeries.push({
        name: currentValue.name,
        data: newData
      });
    });
    new Chartist.Line('#timeChart', {
      series: timeSeries
    }, {
      lineSmooth: Chartist.Interpolation.cardinal({
        fillHoles: true
      }),
      axisX: {
        type: Chartist.FixedScaleAxis,
        divisor: 10,
        labelInterpolationFnc: function(value) {
          return moment(value).format('ddd HH:mm');
        }
      }
    });
  });
}

function getInfluence() {
  httpGetAsync('http://localhost:8080/query/influence', function (response) {
    var data = {
      labels: [],
      series: []
    };
    var options = {
      labelInterpolationFnc: function(value) {
        return value[0];
      }
    };
    var sum = function(a, b) {
      return a + b;
    };
    var responsiveOptions = [
      ['screen and (min-width: 640px)', {
        chartPadding: 30,
        labelOffset: 100,
        labelDirection: 'explode',
        labelInterpolationFnc: function(label, index) {
          return label + ' (' + Math.round(data.series[index] / data.series.reduce(sum) * 100) + '%)';
        }
      }],
      ['screen and (min-width: 1024px)', {
        labelOffset: 40,
        chartPadding: 30
      }]
    ];
    response.forEach(function (current) {
      data.labels.push(current._1);
      data.series.push(parseInt(current._2));
    });
    new Chartist.Pie('#pieChart', data, options, responsiveOptions);
  });
}

/*! Async HTTP */
function httpGetAsync(theUrl, callback) {
  var xmlHttp = new XMLHttpRequest();
  xmlHttp.onreadystatechange = function() {
    if (xmlHttp.readyState === 4 && xmlHttp.status === 200) {
      callback(JSON.parse(xmlHttp.responseText));
    }
  };
  xmlHttp.open('GET', theUrl, true);
  xmlHttp.send(null);
}

