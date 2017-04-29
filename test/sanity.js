'use strict'

require('localenvironment')
require('ngn')
require('ngn-data')
require('../')

let test = require('tape')
let MustHave = require('musthave')
let mh = new MustHave()
let TaskRunner = require('shortbus')

if (!mh.hasAll(process.env, 'DB_HOST', 'DB_USER', 'DB_SECRET')) {
  throw new Error('Testing requires the following environment variables: ' + mh.missing.join(', '))
}

let proxyConfig = {
  host: process.env.DB_HOST,
  username: process.env.DB_USER,
  password: process.env.DB_SECRET,
  autoconnect: false,
  collection: 'test'
}

let meta = function () {
  return {
    idAttribute: 'testid',
    fields: {
      testid: null,
      firstname: null,
      lastname: null,
      val: {
        min: 10,
        max: 20,
        default: 15
      }
    }
  }
}

let Pet = new NGN.DATA.Model({
  fields: {
    name: null,
    breed: null
  }
})

let createPetSet = function () {
  let m = meta()

  m.relationships = {
    pet: Pet
  }

  let NewModel = new NGN.DATA.Model(m)
  let dataset = new NGN.DATA.Store({
    model: NewModel,
    proxy: new NGNX.DATA.MongoDBProxy(proxyConfig)
  })

  try {
    dataset.add({
      firstname: 'The',
      lastname: 'Doctor',
      pet: {
        name: 'K-9',
        breed: 'Robodog'
      }
    })
  } catch (e) {
    console.log('PROBLEM ADDING FIRST RECORD')
    console.log(e)
  }

  try {
    dataset.add({
      testid: 'damaster',
      firstname: 'The',
      lastname: 'Master',
      pet: {
        name: 'Drums',
        breed: '?'
      }
    })
  } catch (e) {
    console.log('PROBLEM ADDING SECOND RECORD')
    console.log(e)
  }

  return dataset
}

const clearDB = function (callback) {
  proxyConfig.poolconnections = false
  let proxy = new NGNX.DATA.MongoDBProxy(proxyConfig)
  proxy.destroy(callback)
}

test('Primary Namespace', function (t) {
  t.ok(NGNX.DATA.MongoDBProxy !== undefined, 'NGNX.DATA.MongoDBProxy is defined globally.')
  t.end()
})

test('Self Inspection & Basic Connectivity', {
  timeout: 8000
}, function (t) {
  let m = meta()
  let NewModel = new NGN.DATA.Model(m)

  proxyConfig.heartbeat = 1500

  let dataset = new NGN.DATA.Store({
    model: NewModel,
    proxy: new NGNX.DATA.MongoDBProxy(proxyConfig)
  })

  t.ok(dataset.proxy.type === 'store', 'Recognized store.')

  m.proxy = new NGNX.DATA.MongoDBProxy(proxyConfig)

  let TestRecord = new NGN.DATA.Model(m)
  let rec = new TestRecord({
    firstname: 'The',
    lastname: 'Doctor'
  })

  t.ok(rec.proxy.type === 'model', 'Recognized model.')
  t.ok(typeof rec.proxy.destroy === 'function', 'Ability to destroy a DB.')

  // After two heartbeats, disconnect
  dataset.proxy.threshold('heartbeat', 2, function () {
    t.pass('Heartbeats triggered successfully.')
    dataset.proxy.disconnect()
  })

  dataset.proxy.once('connected', function () {
    t.pass('Connected to remote host.')

    dataset.proxy.once('disconnected', function () {
      t.pass('Disconnected from remote host.')
      t.end()
    })
  })

  dataset.proxy.connect()
})

test('Clear DB', function (t) {
  clearDB(() => {
    t.pass('Destroy callback executed')
    t.end()
  })
})

test('Basic Save & Fetch (Single Data Model)', function (t) {
  let m = meta()

  m.relationships = {
    pet: Pet
  }

  let pconfig = proxyConfig
  pconfig.poolconnections = false
  m.proxy = new NGNX.DATA.MongoDBProxy(pconfig)

  let DataRecord = new NGN.DATA.Model(m)
  let record = new DataRecord({
    testid: 'docktor',
    firstname: 'The',
    lastname: 'Doctor',
    pet: {
      name: 'K-9',
      breed: 'Robodog'
    }
  })

  record.proxy.save(() => {
    t.pass('Save method applies callback.')

    // Make a change to assure reloading from source will work.
    record.firstname = 'No_name'

    record.once('fetch', () => {
      t.pass('Fetch event triggered.')
      t.ok(record.firstname === 'The', 'Retrieved data from Mongo and populated the record with the result.')
      t.end()
    })

    record.proxy.fetch(() => {
      t.pass('Fetch callback executed.')
    })
  })
})

test('Basic Save & Fetch (Data Store)', function (t) {
  clearDB(() => {
    let ds = createPetSet()

    /**
     * Multiple save and fetch processes are tested
     * to assure ID's persist between changes.
     */
    ds.proxy.save(() => {
      t.pass('Save method applies callback.')

      ds.first.firstname = 'Billy'
      ds.last.firstname = 'Bob'

      let firstId = ds.first.__mongoid
      let lastId = ds.last.__mongoid

      ds.proxy.fetch(() => {
        t.pass('Fetch callback executed.')
        t.ok(ds.first.firstname === 'The', 'Retrieved data from Mongo and populated the record with the result.')
        t.ok(ds.first.__mongoid.toString() === firstId.toString() && ds.last.__mongoid.toString() === lastId.toString(), 'Mongo ID\'s populated correctly.')

        ds.first.firstname = 'Billy Bob'
        ds.proxy.save(() => {
          ds.proxy.fetch(() => {
            t.ok(ds.first.firstname === 'Billy Bob', 'Multiple changes persisted correctly.')
            t.ok(ds.first.__mongoid.toString() === firstId.toString() && ds.last.__mongoid.toString() === lastId.toString(), 'Mongo ID\'s populated correctly after multiple updates.')
            t.end()
          })
        })
      })

      ds.on('fetch', () => {
        t.pass('Fetch event triggered.')
      })
    })
  })
})

test('Store Array Values', function (t) {
  clearDB(() => {
    let Model = new NGN.DATA.Model({
      fields: {
        a: Array
      },
      proxy: new NGNX.DATA.MongoDBProxy(proxyConfig)
    })

    let record = new Model({
      a: ['a', 'b', 'c', {d: true}]
    })

    record.proxy.save(() => {
      t.pass('Saved array data.')
      record.a = []

      record.proxy.fetch(() => {
        t.pass('Retrieved array data.')

        t.ok(Array.isArray(record.a), 'Record returned in array format.')
        t.ok(typeof record.a.pop() === 'object' && record.a[0] === 'a', 'Array data is in correct format.')

        t.end()
      })
    })
  })
})

test('Non-String Primitive Data Types', function (t) {
  clearDB(() => {
    let Model = new NGN.DATA.Model({
      fields: {
        b: Boolean,
        n: Number,
        nil: null,
        o: Object
      },
      proxy: new NGNX.DATA.MongoDBProxy(proxyConfig)
    })

    let record = new Model({
      b: false,
      n: 3,
      o: {
        some: 'value'
      }
    })

    record.proxy.save(() => {
      record.b = true

      record.proxy.fetch(() => {
        t.ok(record.b === false, 'Boolean supported.')
        t.ok(record.n === 3, 'Number supported.')
        t.ok(record.nil === null, 'Null supported.')
        t.ok(record.o.some === 'value', 'Object/JSON supported for models.')
        t.end()
      })
    })
  })
})

test('Restoring Soft Delete Records', function (t) {
  clearDB(() => {
    let Person = new NGN.DATA.Model({
      fields: {
        firstname: null,
        lastname: null
      }
    })

    let People = new NGN.DATA.Store({
      model: Person,
      proxy: new NGNX.DATA.MongoDBProxy(proxyConfig),
      softDelete: true,
      softDeleteTtl: 2000
    })

    People.once('load', () => {
      People.once('save', () => {
        People.proxy.enableLiveSync()

        People.once('live.delete', (record) => {
          People.proxy.collection.count({}).then((count) => {
            t.ok(count === People.recordCount, 'Mongo contains the proper number of records.')

            People.once('live.create', (newRecord) => {
              People.proxy.collection.count({}).then((count) => {
                t.ok(count === People.recordCount && People.recordCount === 2, 'Data store restoration process persists proper number of records to disk.')
                clearDB(() => {
                  People.proxy.once('disconnected', () => {
                    t.end()
                  })

                  People.proxy.disconnect()
                })
              })
            })

            People.restore(record.checksum)
          }).catch((e) => console.log(e))
        })

        People.remove(1)
      })

      People.proxy.save()
    })

    People.load([{
      firstname: 'The',
      lastname: 'Doctor'
    }, {
      firstname: 'The',
      lastname: 'Master'
    }])
  })
})

// test('Expand nested objects to independent collections', function (t) {
//   proxyConfig.fieldAsRecord = false
//   proxyConfig.expandRelationships = true
//
//   let Toy = new NGN.DATA.Model({
//     fields: {
//       name: null,
//       type: null,
//       maker: null
//     }
//   })
//
//   let Pet = new NGN.DATA.Model({
//     fields: {
//       name: null,
//       type: null
//     },
//
//     relationships: {
//       toys: [Toy]
//     },
//
//     dataMap: {
//       toy: 'favorite_toy'
//     }
//   })
//
//   proxyConfig.poolconnections = false
//   let Person = new NGN.DATA.Model({
//     fields: {
//       firstname: null,
//       lastname: null
//     },
//
//     relationships: {
//       pets: [Pet]
//     },
//
//     proxy: new NGNX.DATA.MongoDBProxy(proxyConfig)
//   })
//
//   let p = new Person({
//     firstname: 'The',
//     lastname: 'Doctor',
//     pets: [{
//       name: 'K-9',
//       type: 'dog',
//       toys: [{
//         name: 'Galaxy',
//         type: 'Cosmic',
//         maker: 'Unknown'
//       }]
//     }, {
//       name: 'Garfield',
//       type: 'cat',
//       toys: [{
//         name: 'Odie',
//         type: 'Animal',
//         maker: 'Pet Store'
//       }, {
//         name: 'Spaghetti',
//         type: 'food',
//         maker: 'John'
//       }]
//     }]
//   })
//
//   clearDB(() => {
//     p.proxy.save(() => {
//       p.pets.clear()
//       p.firstname = 'Da'
//       p.lastname = 'Master'
//
//       p.proxy.fetch(() => {
//         t.ok(p.firstname === 'The' && p.lastname === 'Doctor', 'Primary data object retrieved successfully.')
//         t.ok(p.pets.recordCount === 2, 'Correct number of nested objects detected.')
//         t.ok(p.pets.last.toys.recordCount === 2, 'Multiple layers of nesting retrieved from multiple collections.')
//         t.ok(p.pets.last.toys.first.name === 'Odie', 'Multiple layers of nesting returned accurate data from multiple collections.')
//
//         t.end()
//       })
//     })
//   })
// })

// Error.stackTraceLimit = Infinity
test('Live Sync Model', function (t) {
  clearDB(() => {
    let Pet = new NGN.DATA.Model({
      fields: {
        name: null,
        breed: null
      }
    })

    let m = meta()

    m.relationships = {
      pet: Pet
    }

    proxyConfig.poolconnections = true
    m.proxy = new NGNX.DATA.MongoDBProxy(proxyConfig)

    let TempDataRecord = new NGN.DATA.Model(m)
    let record = new TempDataRecord({
      firstname: 'The',
      lastname: 'Doctor',
      pet: {
        name: 'K-9',
        breed: 'Robodog'
      }
    })

    record.proxy.save(() => {
      record.proxy.enableLiveSync()
      let tasks = new TaskRunner()

      tasks.add((next) => {
        record.proxy.once('live.update', () => {
          t.pass('live.update method detected.')

          record.setSilent('firstname', 'Bubba')

          record.proxy.fetch(() => {
            t.ok(record.firstname === 'Da', 'Persisted correct value.')
            next()
          })
        })

        record.firstname = 'Da'
      })

      tasks.add((next) => {
        record.proxy.once('live.create', () => {
          t.pass('live.create triggered on new field creation.')
          record.proxy.fetch(() => {
            t.ok(record.hasOwnProperty('middlename') && record.middlename === 'Alonsi', 'Field creation persisted on the fly.')
            next()
          })
        })

        record.addField('middlename', {
          type: String,
          default: 'Alonsi',
          required: true
        })
      })

      tasks.add((next) => {
        record.proxy.once('live.delete', () => {
          t.pass('live.delete triggered on new field creation.')
          t.ok(!record.hasOwnProperty('middlename'), 'Field deletion persisted on the fly.')
          next()
        })

        record.removeField('middlename')
      })

      tasks.add((next) => {
        record.proxy.once('live.update', () => {
          t.pass('live.update triggered when new relationship is available.')

          record.vehicle.setSilent('type', 'other')

          record.proxy.fetch(() => {
            t.ok(record.vehicle.type === 'Tardis', 'Proper value persisted in nested model.')
            next()
          })
        })

        let Vehicle = new NGN.DATA.Model({
          fields: {
            type: null,
            doors: Number
          }
        })

        record.on('relationship.create', () => {
          record.vehicle.type = 'Tardis'
        })

        record.addRelationshipField('vehicle', Vehicle)
      })

      tasks.on('complete', () => {
        record.proxy.once('disconnected', () => t.end())
        record.proxy.disconnect()
      })

      tasks.run(true)
    })
  })
})

test('Live Sync Store', function (t) {
  clearDB(() => {
    let Person = new NGN.DATA.Model({
      fields: {
        firstname: null,
        lastname: null
      }
    })

    proxyConfig.poolconnections = true
    let People = new NGN.DATA.Store({
      model: Person,
      proxy: new NGNX.DATA.MongoDBProxy(proxyConfig)
    })

    People.proxy.enableLiveSync()

    let tasks = new TaskRunner()

    tasks.add((next) => {
      People.proxy.once('live.create', (record) => {
        t.ok(People.first.lastname === record.lastname, 'Persisted record and local record are equivalent.')
        t.ok(record.firstname === 'The' && record.lastname === 'Doctor', 'Correct values stored for first record.')

        next()
      })

      People.add({
        firstname: 'The',
        lastname: 'Doctor'
      })
    })

    tasks.add((next) => {
      People.proxy.once('live.create', (record) => {
        t.ok(record.firstname === 'The' && record.lastname === 'Master', 'Correct values stored for multiple records.')

        next()
      })

      People.add({
        firstname: 'The',
        lastname: 'Master'
      })
    })

    tasks.add((next) => {
      People.proxy.once('live.update', (record) => {
        t.ok(record.firstname === 'Da' && record.lastname === 'Master', 'Correct record and value written during update.')

        next()
      })

      People.last.firstname = 'Da'
    })

    tasks.add((next) => {
      People.proxy.once('live.delete', (record) => {
        People.proxy.collection.count({
          _id: record.__mongoid
        }).then((count) => {
          t.ok(count === 0, 'Deleted record does not exist in the database.')
          next()
        })
      })

      People.remove(People.first)
    })

    tasks.add((next) => {
      People.proxy.once('live.delete', () => {
        People.proxy.collection.count({}).then((count) => {
          t.ok(count === 0, 'Cleared record does not exist in the database.')
          next()
        })
      })

      People.clear()
    })

    tasks.on('complete', () => {
      clearDB(() => {
        People.proxy.once('disconnected', () => t.end())
        People.proxy.disconnect()
      })
    })

    tasks.run(true)
  })
})
