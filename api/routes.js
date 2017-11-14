const apis = require('./apis')
const methods = require('./methods')
const router = require('koa-router')()

const baseUrl = '/api'

apis.forEach((request) => {
    const method = request.method.toLowerCase()
    if (!methods.includes(method)) {
        return
    }

    router[method].call(router, `${baseUrl}${request.url}`,async (ctx,next)=>{
        await request.handler(ctx)
    })
})

module.exports = router