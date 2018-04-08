/**
 * @author  xing7th@gmail.com
 * @website http://www.showdoc.cc/htq
 */
// (1) variables
var redis = require('redis');
var request = require('request');
var fs = require('fs');
var config = JSON.parse(fs.readFileSync('./config.json').toString());
var redis_client = redis.createClient(config.redis_port, config.redis_host); //creates a new client

// (2) rpc server
var hprose = require("hprose");
var nginxSuffix = '/rpc'; // used for proxy & load balance
var server = new hprose.Server("http://0.0.0.0:" + config.api_port + nginxSuffix);

// (3) rpc methods
//添加队列
function addQueue(params, callback) {
  console.log(params);
  var queue_name = config.redis_key_prefix + params.queue_name;
  var type = params.type;
  var post_app_key = config.app_key;
  var post_app_token = config.app_token;
  var stepping_time = params.stepping_time;
  var max_time_interval = params.max_time_interval;
  var attribute = { "type": type };
  if (stepping_time) { attribute['stepping_time'] = stepping_time };
  if (max_time_interval) { attribute['max_time_interval'] = max_time_interval };

  if (!check_token(post_app_key, post_app_token)) {
    callback({"error_code":1000,"message":"认证失败"});
    return false;
  }
  //判断是否在哈希表里存在队列名
  redis_client.hexists(config.redis_key_prefix + "queue_list", queue_name, function (err, reply) {
    if (!reply) {
      //增加
      redis_client.hmset(config.redis_key_prefix + "queue_list", queue_name, JSON.stringify(attribute), function (err, reply) {
        if (reply) {
          callback({"error_code":0,"message":"添加成功"});
        } else {
          callback({"error_code":1001,"message":"添加失败"});
        }
      });
    } else {
      callback({"error_code":1002,"message":"队列名已经存在"});
    }
  });
}

//删除队列
function delQueue(params, callback) {
  var queue_name = config.redis_key_prefix + params.queue_name;
  var type = params.type;
  var post_app_key = config.app_key;
  var post_app_token = config.app_token;
  if (!check_token(post_app_key, post_app_token)) {
    callback({"error_code":1000,"message":"认证失败"});
    return false;
  }
  //判断是否在哈希表里存在队列名
  redis_client.hexists(config.redis_key_prefix + "queue_list", queue_name, function (err, reply) {
    if (reply) {
      redis_client.del(queue_name);
      redis_client.del(queue_name + "_times");
      redis_client.hdel(config.redis_key_prefix + "queue_list", queue_name, function (err, reply) {
        redis_client.del(queue_name);
        if (reply) {
          callback({"error_code":0,"message":"删除成功"});
        } else {
          callback({"error_code":1004,"message":"删除失败"});
        }

      });
    } else {
      callback({"error_code":1003,"message":"队列不存在，请先创建"});
    }
  });
}

//获取所有队列
function getAllQueue(params, callback) {
  var queue_name = config.redis_key_prefix + params.queue_name;
  var type = params.type;
  var post_app_key = config.app_key;
  var post_app_token = config.app_token;
  if (!check_token(post_app_key, post_app_token)) {
    callback({"error_code":1000,"message":"认证失败"});
    return false;
  }

  redis_client.hscan(config.redis_key_prefix + "queue_list", 0, function (err, reply) {
    var queue_list = reply[1];
    var return_queue_list = [];
    if (queue_list) {
      for (var i = 0; i < queue_list.length; i = i + 2) {
        var queue_name = queue_list[i];
        queue_name = queue_name.replace(config.redis_key_prefix, '');
        var attribute = JSON.parse(queue_list[i + 1]);
        var queue = { "queue_name": queue_name, "attribute": attribute };
        return_queue_list.push(queue);
      };
    };
    callback({"error_code":0,"queue_list":return_queue_list});
  });
}

//获取某个队列当前的任务数
function countQueueTasks(params, callback) {
  var queue_name = config.redis_key_prefix + params.queue_name;
  var type = params.type;
  var post_app_key = config.app_key;
  var post_app_token = config.app_token;
  if (!check_token(post_app_key, post_app_token)) {
    callback({"error_code":1000,"message":"认证失败"});
    return false;
  }

  redis_client.zcard(queue_name, function (err, reply) {
    //res.send(reply);
    var count = parseInt(reply);
    callback({"error_code":0,"count":count});
  });
}

//为某个队列添加任务
function addTask(params, callback) {
  var queue_name = config.redis_key_prefix + params.queue_name;
  var url = params.url;
  var post_app_key = config.app_key;
  var post_app_token = config.app_token;
  var execute_time = params.execute_time;
  if (!check_token(post_app_key, post_app_token)) {
    callback({"error_code":1000,"message":"认证失败"});
    return false;
  }
  //判断是否在哈希表里存在队列名
  redis_client.hexists(config.redis_key_prefix + "queue_list", queue_name, function (err, reply) {
    if (!reply) {
      callback({"error_code":1003,"message":"队列不存在，请先创建"});
    } else {
      if (execute_time) {
        var score = Date.parse(new Date(execute_time));
      } else {
        var score = Date.parse(new Date());
      }

      if (url.indexOf("?") > -1) {
        url += "&htq_no_repeat=" + score + Math.random().toFixed(4);
      } else {
        url += "?htq_no_repeat=" + score + Math.random().toFixed(4);
      }
      redis_client.zadd(queue_name, score, url, function (err, reply) {
        if (reply) {
          callback({"error_code":0,"message":"添加成功"});
        } else {
          callback({"error_code":1001,"message":"添加失败"});
        }

      });
    }
  });
}

function check_token(post_app_key, post_app_token) {
  if (post_app_key == config.app_key && post_app_token == config.app_token) {
    return true;
  } else {
    return false;
  }
}

// (4) add to rpc server
server.addFunction(addQueue, { async: true });
server.addFunction(delQueue, { async: true });
server.addFunction(getAllQueue, { async: true });
server.addFunction(countQueueTasks, { async: true });
server.addFunction(addTask, { async: true });

server.on('beforeInvoke', function(name, args, byref, context) {
  console.log('beforeInvoke',name);
  console.log(name, args, byref);
});

server.on('afterInvoke', function(name, args, byref, result, context) {
  // console.log(name, args, byref, result);
  console.log('afterInvoke',name,result);
});

server.start();

