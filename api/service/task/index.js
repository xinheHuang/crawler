/**
 * Created by Xinhe on 2017-09-20.
 */
const ApiError = require('../../../error/ApiError')
const ApiErrorNames = require('../../../error/ApiErrorNames')
const child_process = require('child_process')
const scriptConfig= require ('../../../conf/scriptConfig')
const mqConfig = require('../../../conf/mqConfig')

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
    static async startTask(taskId,type,filename,args) {
        const channel =await TaskService.getChannel();
        try {
            // const spawnObj = child_process.spawn(`${type} ${scriptConfig.path}${filename} ${args}`,{
            //     cwd: root
            // });
            console.log(type,scriptConfig.path,filename,args)
            const spawnObj =child_process.spawn(type,[`${scriptConfig.path}${filename}`,...(args.split(' '))])
            spawnObj.stdout.on('data', chunk => {
                const msg = chunk.toString();
                console.log('stdout',msg);
                channel.sendToQueue(queue, new Buffer(msg))
            });
            spawnObj.stderr.on('data', (data) => {
                console.log('stderr',data);
            });
            spawnObj.on('close', code => {
                console.log('close code : ' + code);
            })
            spawnObj.on('exit', (code) => {
                console.log('exit code : ' + code);
            });
            TaskService.process.set(taskId,spawnObj)
        } catch (e) {
            console.log('spawn error',e)
        }
    }

    static async stopTask(taskId) {
       const proc= TaskService.process.get(taskId);
       if (proc){
           proc.kill();
       }
    }
}

TaskService.channel=null;
TaskService.process=new Map();
module.exports = TaskService
