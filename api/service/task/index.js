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
const kill = require('tree-kill');

//amqp
const { queue, username, password, host, port } = mqConfig
const open = require('amqplib')
    .connect(`amqp://${username}:${password}@${host}:${port}`)

const isWin=process.platform === 'win32';

const slash = isWin? '\\':'/'
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
            const cwd=basepath.substr(0,basepath.indexOf('bin'))+'scripts'+slash;
            const filepath=filename;
            const args= [filepath,...(extraArgs.split(' '))]
            let scriptType=type;
            if (type.indexOf('python')>=0){
                args.unshift('-u','-W ignore')
                if (isWin){
                    scriptType = 'python'
                }  else{
                    scriptType = 'python3'
                }
            }
            console.log(scriptType,args,cwd);
            const spawnObj =child_process.spawn(scriptType,args,{
            	shell:true,
            	// detached: true,
                cwd
            })
            spawnObj.stdout.on('data', chunk => {
                const r=chunk.toString()
                r.split(/[\r\n]+/).filter((d)=>d).forEach(msg=>{
                    channel.sendToQueue(queue, new Buffer(Message(taskId,subtaskId,Message.type.LOG, msg,filename)))
                })
            });
            spawnObj.stderr.on('data', (chunk) => {
                const error =chunk.toString();
                console.log('error',error)
                const msg=Message(taskId,subtaskId,Message.type.ERROR,error,filename);
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
       if (proc && proc.pid){
       	   console.log('killing '+proc.pid)
           kill(proc.pid)
           // process.kill(-proc.pid);
       }
    }
}

TaskService.channel=null;
TaskService.process=new Map();
module.exports = TaskService
