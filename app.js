const Koa = require('koa')
const app = new Koa()
const json = require('koa-json')
const bodyparser = require('koa-bodyparser')
const logger = require('koa-logger')
const config = require('./conf/config')
const logUtil = require('./utils/logUtil')
const responseFormatter = require('./middlewares/responseFormatter')
const apis = require('./api/routes')


app.keys = config.keys

app.use(bodyparser({
    enableTypes: ['json', 'form', 'text']
}))
app.use(json())
app.use(logger())

// logger
app.use(async (ctx, next) => {
    const start = new Date()
    try {
        await next()
        const ms = new Date() - start
        logUtil.logResponse(ctx, ms)
    } catch (error) {
        const ms = new Date() - start
        //记录异常日志
        logUtil.logError(ctx, error, ms)
    }
})

app.use(responseFormatter('^/api'))

// routes
app.use(apis.routes(), apis.allowedMethods())

app.on('error', function (err, ctx) {
    console.log(err)
})

module.exports = app
