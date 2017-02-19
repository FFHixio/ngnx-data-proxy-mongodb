# NGNX.DATA.MongoProxy

`npm i ngnx-data-proxy-mongo`

```js
require('ngnx-data-proxy-mongo')

const Person = new NGN.DATA.Model({
  fields: {
    firstname: null,
    lastname: null
  },

  proxy: new NGNX.DATA.MongoProxy({
    host: 'host.com:12345',
    database: 'people_db',
    username: 'user',
    password: 'password'
  })
})
```

The MongoDB proxy is used to perform CRUD operations from an NGN.DATA.Store and/or
NGN.DATA.Model.  
