var jargs = require('./argsfile.json');
var moment = require('moment');
var express = require('express');
var app = express();
const multer = require('multer');
const fs = require('fs');
const got = require('got');
const md5 = require('md5');
const path = require("path");
const bodyParser = require('body-parser');
const urlencodedParser = bodyParser.urlencoded({ extended: false });
const { pipeline } = require('node:stream');
const async=require('promise-async');
const axios = require('axios');

const redis = require('redis');
const redisClient = redis.createClient();
redisClient.connect(jargs.redisport, jargs.redisip)  .then(() => { console.log("Redis已链接"); });

app.post('/upload_chunks', multer({dest: jargs.chunkDir}).array('file'), function (req, res, next) {
	let obj= req.body;
	let chunkpath = req.files[0].path;// 原始片段在临时目录下的路径
	let chunkid = (obj.chunk)?Number(obj.chunk):0;
	redisClient.lPush(obj.reqid+":"+obj.name,chunkid+":"+jarg.nodeid+":"+chunkpath);
	return res.json({'status': 1, 'msg': obj.fileuid + '_' + obj.chunk + '上传成功!'});
});

function mergechunk(itemstr, targetFile){
	let items=itemstr.split(":");
	let start = items[0] * jargs.chunkSize;
	return new Promise((resolve,reject)=>{
		pipeline( 
			(items[1]==jargs.nodeid) ? fs.createReadStream(chunkpath) : got.stream(jargs.weburl[items[1]]+items[2]), 
			fs.createWriteStream(targetFile, { flags:'r+', start:start }), (err) => { if (err) { reject(err);}  
				(items[1]==jargs.nodeid) ? fs.unlink(jargs.chunkDir+items[2],err=>{ if (err) reject(err); resolve(); }) : axios.get(jargs.remotedel[items[1]]+"?chunkpath="+items[2]);
				});
		});
}

app.post('/merge_chunks',urlencodedParser, function (req, res, next) {

	let obj= req.body;
	let dest_dir = jargs.destDir+"/"+obj.reqid;
	fs.mkdir(dest_dir, { recursive: true }, (err) => {
		if (err) { console.log(err); return res.json({'status': 0, 'filename': ''}); }
		else {
			let dest_path = dest_dir+"/"+ obj.name;
			fs.writeFile(dest_path, '', (error) => { if (error) { console.log("创建文件失败");  } else { 
				let filemap=redisClient.lrange(obj.reqid+":"+obj.name);
				let mergePromise = async.mapLimit(filemap, jargs.mclimit, function(item, callback){ mergechunk(item,dest_path).then((resolve) => { callback(null, item); }); });
				mergePromise.then((result) => { 
					redisClient.del(obj.reqid+":"+obj.name);
					return res.json({ 'status': 1, 'filename': encodeURI(obj.name) });
					}).catch(e =>{ console.log("merge fail"); return res.json({'status': 0, 'filename': ''}); });
				}});
			}
		});
});

app.get('/del_remote_chunk',function (req, res, next) {
	let args=req.query;
	fs.unlink(jargs.chunkDir+args.chunkpath,err=>{ if (err) config(err); })
});

var argvport=(process.argv.length==2)?jargs.defaultport:eval(process.argv.slice(2)[0]);
var server=app.listen(argvport,function(){
	console.log(moment().format("YYYY/MM/DD HH:mm:ss")+" 文件分片上传&合并接口启动 @port "+server.address().port);
})
