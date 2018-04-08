var Redis = require('ioredis');
var JSONStore = require('redis-json');

var redis = new Redis(6379, 'wise-found.com');

var jsonStore = new JSONStore(redis);

jsonStore.set('user', {
    a: 1,
    b: 2,
    c : {
        d: 3,
        e: 4
    }
}, function(err, result){
    console.log(err, result);
    jsonStore.get('user',function (err, result) {
        console.log(err, result.c);
    });
});

var fs = require('fs');
var config = JSON.parse(fs.readFileSync('./config.json').toString());
console.log(config.app_key);
var request = require('request');
var url = 'http://127.0.0.1:5999/api/addQueue';
var formData = {
    "app_key":config.app_key
    ,"app_token":config.app_token
    ,"queue_name":"WriteDb"
    ,"type":"real_time"
    // ,"stepping_time":stepping_time
    // ,"max_time_interval":max_time_interval
};

request({
    url: url,
    method: "POST",
    json: true,
    headers: {
        "content-type": "application/json",
    },
    body: formData
}, function(error, response, body) {
    if (!error && response.statusCode == 200) {
        console.log(body);
    }
}); 
