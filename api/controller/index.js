/**
 * Created by Xinhe on 2017-09-20.
 */
const TaskService = require('../service/task')

module.exports = Object.values({
    startTask: {
        method: 'post',
        url: '/task/start',
        async handler(ctx) {
            const { taskId,subtaskId, type, fileName, args } = ctx.request.body
            await TaskService.startTask(taskId,subtaskId, type, fileName, args)
            ctx.body = 'success'
        }
    },
    stopTask: {
        method: 'post',
        url: '/task/stop',
        async handler(ctx) {
            const { subtaskId } = ctx.request.body
            await TaskService.stopTask(subtaskId)
            ctx.body = 'success'
        }
    },

})
