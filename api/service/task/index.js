/**
 * Created by Xinhe on 2017-09-20.
 */
const ApiError = require('../../../error/ApiError')
const ApiErrorNames = require('../../../error/ApiErrorNames')
const child_process = require('child_process')
const scriptConfig= require ('../../../conf/scriptConfig')
const mqConfig = require('../../../conf/mqConfig')
const Message =require ('./Message')
const path = require ('path')
//amqp
const { queue, username, password, host, port } = mqConfig
const open = require('amqplib')
    .connect(`amqp://${username}:${password}@${host}:${port}`)

class TaskService {

    static async getChannel(){
        if (!TaskService.channel){
            const conn = await open;
            const channel = await conn.createChannel();
            await channel.assertQueue(queue);
            TaskService.channel= channel;
        }
        return TaskService.channel
    }
    static async startTask(taskId,subtaskId,type,filename,extraArgs) {
        const channel =await TaskService.getChannel();
        try {
            const basepath=path.dirname(require.main.filename);
            const filepath=basepath.substr(0,basepath.indexOf('bin'))+'scripts/'+filename;
            console.log(type,filepath,extraArgs)
            const args= [filepath,...(extraArgs.split(' '))]
            if (type=='python'){
                args.unshift('-u')
            }
            const spawnObj =child_process.spawn(type,args,{
            	shell:true,
            	detached: true
            })
            spawnObj.stdout.on('data', chunk => {
                console.log('data',chunk.toString('utf8'))
                chunk.toString('utf8').split(/[\r\n]+/).filter((d)=>d).forEach(msg=>{
                    channel.sendToQueue(queue, new Buffer(Message(taskId,subtaskId,Message.type.LOG, msg,filename)))
                })
            });
            spawnObj.stderr.on('data', (data) => {
                console.log('error',data.toString('utf8'))
                const msg=Message(taskId,subtaskId,Message.type.ERROR, data.toString('utf8'),filename);
                channel.sendToQueue(queue, new Buffer(msg))
            });
            spawnObj.on('exit', (code) => {
                console.log('exit',code)
                const msg=Message(taskId,subtaskId,Message.type.DONE, code,filename);
                channel.sendToQueue(queue, new Buffer(msg))
            });
            TaskService.process.set(subtaskId,spawnObj)
        } catch (e) {
            console.log('spawn error',e)
        }
    }

    static async stopTask(subtaskId) {
       const proc= TaskService.process.get(subtaskId);
       if (proc){
       	   console.log('killing')
           process.kill(-proc.pid);
       }
    }
}

TaskService.channel=null;
TaskService.process=new Map();
module.exports = TaskService
