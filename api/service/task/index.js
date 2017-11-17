/**
 * Created by Xinhe on 2017-09-20.
 */
const ApiError = require('../../../error/ApiError')
const ApiErrorNames = require('../../../error/ApiErrorNames')
const child_process = require('child_process')
const scriptConfig= require ('../../../conf/scriptConfig')
const mqConfig = require('../../../conf/mqConfig')
const Message =require ('./Message')
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
    static async startTask(taskId,subtaskId,type,filename,args) {
        const channel =await TaskService.getChannel();
        try {
            // const spawnObj = child_process.spawn(`${type} ${scriptConfig.path}${filename} ${args}`,{
            //     cwd: root
            // });
            console.log(type,scriptConfig.path,filename,args)
            const spawnObj =child_process.spawn(type,[`${scriptConfig.path}${filename}`,...(args.split(' '))])
            spawnObj.stdout.on('data', chunk => {
                console.log('data',chunk.toString())
                const msg=Message(taskId,subtaskId,Message.type.LOG, chunk.toString(),filename);
                channel.sendToQueue(queue, new Buffer(msg))
            });
            spawnObj.stderr.on('data', (data) => {
                console.log('error',data)
                const msg=Message(taskId,subtaskId,Message.type.ERROR, data,filename);
                channel.sendToQueue(queue, new Buffer(msg))
            });
            spawnObj.on('close', code => {
                console.log('close',code)
                const msg=Message(taskId,subtaskId,Message.type.DONE, code,filename);   
                channel.sendToQueue(queue, new Buffer(msg))
            })
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
           proc.kill();
       }
    }
}

TaskService.channel=null;
TaskService.process=new Map();
module.exports = TaskService
