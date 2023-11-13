var jargs = require('./argsfile.json');
var moment = require('moment');
var express = require('express');
var app = express();
const multer = require('multer');
const fs = require('fs');
const md5 = require('md5');
const path = require("path");
const bodyParser = require('body-parser');
const urlencodedParser = bodyParser.urlencoded({ extended: false });
const { pipeline } = require('node:stream');
const async=require('promise-async');

var filechunkpath=[];

app.post('/upload_chunks', multer({dest: jargs.chunkDir}).array('file'), function (req, res, next) {
	let obj= req.body;
	let chunkpath = req.files[0].path;// 原始片段在临时目录下的路径
	let chunkid = (obj.chunk)?Number(obj.chunk):0;
	let chunkitem=JSON.parse(JSON.stringify({"reqid":obj.reqid,"filename":obj.name,"chunkid":chunkid,"chunkpath":chunkpath}));
	filechunkpath.push(chunkitem);
	return res.json({'status': 1, 'msg': obj.fileuid + '_' + obj.chunk + '上传成功!'});
});

function mergechunk(chunkpath, chunkid, targetFile){
	let start = chunkid * jargs.chunkSize;
	return new Promise((resolve,reject)=>{
		pipeline( fs.createReadStream(chunkpath), fs.createWriteStream(targetFile, { flags:'r+', start:start }), (err) => { if (err) { reject(err);}  fs.unlink(chunkpath,err=>{ if (err) reject(err); resolve(); }); });
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
				let filemap=filechunkpath.filter(item=>((item.reqid==obj.reqid)&&(item.filename==obj.name)));
				let mergePromise = async.mapLimit(filemap, jargs.mclimit, function(item, callback){ mergechunk(item.chunkpath, item.chunkid,dest_path).then((resolve) => { callback(null, item); }); });
				mergePromise.then((result) => { 
					filechunkpath=filechunkpath.filter(item=>((item.reqid!=obj.reqid)||(item.filename!=obj.name)));
					return res.json({ 'status': 1, 'filename': encodeURI(obj.name) });
					}).catch(e =>{ console.log("merge fail"); return res.json({'status': 0, 'filename': ''}); });
				}});
			}
		});
});

var argvport=(process.argv.length==2)?jargs.defaultport:eval(process.argv.slice(2)[0]);
var server=app.listen(argvport,function(){
	console.log(moment().format("YYYY/MM/DD HH:mm:ss")+" 文件分片上传&合并接口启动 @port "+server.address().port);
})
