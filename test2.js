var hprose = require("hprose");
var client = hprose.Client.create("http://127.0.0.1:5999/rpc");
var proxy = client.useService();

var data = {};
data.queue_name = 'hello';
data.type = 'real_time';
// data.stepping_time = params.stepping_time;
// data.max_time_interval = params.max_time_interval;
proxy.addQueue(data, function(result) {
    console.log(result);
});

proxy.getAllQueue(data, function(result) {
    console.log(result.queue_list);
});

// proxy.delQueue(data, function(result) {
//     console.log(result.queue_list);
// });

proxy.countQueueTasks(data, function(result) {
    console.log(result);
});

var data2 = {};
data2.queue_name = 'hello';
data2.url = 'http://www.baidu.com';
data2.execute_time = '2018-04-08 23:8:12';
proxy.addTask(data2, function(result) {
    console.log(result);
});



