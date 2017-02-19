# NGNX.DATA.MongoDBProxy

`npm i ngnx-data-proxy-mongodb`

```js
require('ngnx-data-proxy-mongodb')

const Person = new NGN.DATA.Model({
  fields: {
    firstname: null,
    lastname: null
  },

  proxy: new NGNX.DATA.MongoDBProxy({
    host: 'host.com:12345',
    database: 'people_db',
    username: 'user',
    password: 'password'
  })
})
```

The MongoDB proxy is used to perform CRUD operations from an NGN.DATA.Store and/or NGN.DATA.Model.  
