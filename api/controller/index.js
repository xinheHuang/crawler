/**
 * Created by Xinhe on 2017-09-20.
 */
const TaskService = require('../service/task')

module.exports = Object.values({
    startTask: {
        method: 'post',
        url: '/task/start',
        async handler(ctx) {
            const { taskId, type, filename, args } = ctx.request.body
            await TaskService.startTask(taskId, type, filename, args)
            ctx.body = 'success'
        }
    },
    stopTask: {
        method: 'post',
        url: '/task/stop',
        async handler(ctx) {
            const { taskId } = ctx.request.body
            await TaskService.stopTask(taskId)
            ctx.body = 'success'
        }
    },

})
