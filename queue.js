/**
 * @author  xing7th@gmail.com
 * @website http://www.showdoc.cc/htq
 */
// var express = require('express');
// var bodyParser =  require("body-parser"); 
var redis = require('redis');
var request = require('request');
var fs = require('fs');
var config = JSON.parse(fs.readFileSync('./config.json').toString());
var redis_client = redis.createClient(config.redis_port,config.redis_host); //creates a new client
console.log("后台队列服务已经启动，随时等待新队列任务");
var queue_status_array = [];

//定时循环读取redis。不用担心会无序并发运行，后面会根据queue_status_array来控制同一时间只操作一个队列
setInterval(function(){
	check_queue_hash();
},1000);


//扫描整个队列哈希表
function check_queue_hash(){
	redis_client.hscan(config.redis_key_prefix+"queue_list",0,function(err, reply){
		var queue_list = reply[1] ;
		if (queue_list) {
			for (var i = 0; i < queue_list.length; i=i+2) {
				var single_queue_name = queue_list[i];
				var single_attribute = JSON.parse(queue_list[i+1]);
				//判断队列是否在操作中。如果是则不重复启动，以保证同一个队列内的任务是按顺序one by one 执行的
				if (queue_status_array &&  queue_status_array[single_queue_name] > 0 ) {
					////console.log('队列'+single_queue_name+'正操作中，不重复启动');
				}else{
					//执行单个队列single_queue_name
					run_queue(single_queue_name,single_attribute);
				}
				
			};
		};

	});
}


//执行单个队列
function run_queue(queue_name,attribute){
	queue_status_array[queue_name] = 1 ;
	//获取有序集合里的score最小并且score小于当前时间戳的一个元素
	var cur_time = Date.parse(new Date());
	redis_client.zrangebyscore(queue_name,0,cur_time,'LIMIT',0,1,function(err, reply){
		var url = reply[0] ;
		if (url &&  url!= '' &&  url!= 'undefined' ) {
			//为了防止redis元素重复，在添加url的时候自动加了些随机数。现在需要去掉随机数才是真正的访问url
			var request_url = url.substring(0 ,url.indexOf("htq_no_repeat=")-1 );
			//如果是可变队列
			if (attribute.type == 'variable') {
				times_queue = queue_name+"_times";
				//获取执行次数
				redis_client.hget(times_queue,url,function(err, reply){
					var execution_times = parseInt(reply) ? parseInt(reply) : 0 ;
					request(request_url, function (error, response, body) {
						////console.log("第"+(execution_times+1)+"次执行来自"+queue_name+"的url："+request_url);
						if (body == "done") {
							//删除任务
							redis_client.zrem(queue_name,url);
							redis_client.hdel(times_queue,url);
						}
						else if (body == "reset") {
							//重置
							redis_client.hset(times_queue,url,0);
							next_time = Date.parse(new Date()) + attribute.stepping_time*1000 ;
							redis_client.zadd(queue_name,next_time,url);
							
						}else{
							var offset = execution_times*attribute.stepping_time*1000 ;
							offset = offset > attribute.max_time_interval*1000 ? attribute.max_time_interval*1000 :offset ;
							next_time = Date.parse(new Date()) + offset ;
							////console.log("推迟到"+next_time+"执行");
							redis_client.zadd(queue_name,next_time,url);
							redis_client.hset(times_queue,url,execution_times+1);

						}
						//queue_status_array[queue_name] = 0 ;
						run_queue(queue_name,attribute);
						return ;
					});
				});



			}else{
			 //如果是实时队列或者定时队列
				//删除这个元素。不在执行url后在删除是为了防止因为执行不了url而造成阻塞
				redis_client.zrem(queue_name,url,function(){
					request(request_url, function (error, response, body) {
						//console.log("执行来自"+queue_name+"的url："+request_url);
						//queue_status_array[queue_name] = 0 ;
						run_queue(queue_name,attribute);
						return ;
					});
				});
			}



		}else{
			//这里是结束递归的关键：不再返回元素，集合为空或者score小于当前时间戳的集合为空
			queue_status_array[queue_name] = 0 ;
			return false;
		}

	});	
}
