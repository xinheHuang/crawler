/**
 * Created by Xinhe on 2017-11-16.
 */
const Message = (taskId, subtaskId, type, message,scriptName) => JSON.stringify({
    taskId,
    subtaskId,
    type,
    message,
    scriptName
})
Message.type = {
    LOG: 'LOG',
    DONE: 'DONE',
    ERROR: 'ERROR'
}
module.exports = Message