'use strict'

require('ngnx-data-proxy-database')
const TaskRunner = require('shortbus')

/**
 * @class NGNX.DATA.MongoProxy
 * Persist NGN DATA stores using a MongoDB.
 * @fires connected
 * Fired when the database connection is established.
 * @fires disconnected
 * Fired when the database connection is dropped.
 * @fires reconnected
 * Fired when the database connection is reestablished.
 * @fires heartbeat
 * Fired when the heartbeat is sent.
 * @fires collection.changed
 * Fired when the collection attribute is changed. Handler
 * methods will receive an object with an `old` and `new`
 * key, each containing the old/new collection name.
 */
class MongoProxy extends NGNX.DATA.DatabaseProxy {
  /**
   * @constructor
   * Create a new instance of the proxy.
   * @param {object} configuration
   * The configuration object.
   * @param {Function} [callback]
   * An optional callback, executed when a connection to the remote MongoDB
   * server has been established.
   */
  constructor (cfg, callback) {
    cfg = cfg || {}
    super(cfg)

    cfg.host = NGN.coalesce(cfg.host, 'localhost')

    Object.defineProperties(this, {
      /**
       * @cfg {boolean} [autoconnect=true]
       * Automatically attempt to connect to the MongoDB server.
       * This is ignored if #poolconnections is set to `false`.
       */
      autoconnect: NGN.privateconst(NGN.coalesce(cfg.autoconnect, true)),

      /**
       * @cfg {boolean} [autoreconnect=true]
       * Automatically attempt to reconnect to the MongoDB server whenever the
       * connection is lost.
       *
       * This is ignored if #poolconnections is set to `false`.
       */
      autoreconnect: NGN.privateconst(NGN.coalesce(cfg.autoreconnect, true)),

      /**
       * @cfg {string} collection
       * The primary collection to proxy to.
       */
      _collection: NGN.private(cfg.collection || 'unknown'),

      /**
       * @cfg {boolean} [expandRelationships=false]
       * When `true`, any relationship datafields will be saved/fetched
       * from their own collection.
       *
       * For example:
       *
       * ```
       * let MyModel = new NGNX.DATA.Model({
       *   fields: {...},
       *   relationships: {
       *     anothercollection: SubModel
       *   }
       * })
       * ```
       *
       * When `MyModel` is persisted, it will attempt to upsert
       * a record in the `anothercollection` collection, using
       * the data from `SubModel` as it's value.
       */
      expandRelationships: NGN.private(NGN.coalesce(cfg.expandRelationships, false)),

      /**
       * @cfg {string} [host=localhost]
       * The host can be just the server name/URI or the URI+Port. For example,
       * `myserver.com` and `myserver.com:12345` are both valid.
       */
      host: NGN.privateconst(cfg.host.split(':')[0]),

      /**
       * @cfg {number} [port=27017]
       * The port number. This overrides any port setting in the #host.
       */
      port: NGN.privateconst(parseInt(NGN.coalesce(
        cfg.port,
        cfg.host.split(/[\:\\\/]/).length > 1 // eslint-disable-line
          ? cfg.host.split(/[\:\\\/]/gi)[1] // eslint-disable-line
          : null,
        27017), 10)),

      /**
       * @cfg {string} username
       * The username to connect to the #host with.
       */
      user: NGN.privateconst(NGN.coalesce(cfg.username, 'unknown')),

      /**
       * @cfg {string} password
       * The password to connect to the #host with.
       */
      _credential: NGN.private(NGN.coalesce(cfg.password, cfg.secret)),

      /**
       * @cfg {string} database
       * The name of the database on the #host to connect to.
       */
      db: NGN.privateconst(NGN.coalesce(cfg.database, cfg.db, cfg.host.split('/').pop(), 'unknown')),

      /**
       * @cfg {boolean} [poolconnections=true]
       * By default, a connection pool is created, meaning one connection
       * is established and shared for all operations performed by the proxy.
       * The pooled connection will remain active until the #disconnect method
       * is executed.
       *
       * Setting this to `false` will disable connection pooling. Without pooling,
       * each operationg (save, fetch, etc) will create a new connection before
       * it runs, discarding the connection upon completion. For highly interactive
       * applications that communicate regularly with the database, disabling the
       * connection pool can result in very inefficient computing.
       *
       * Pooling is recommended in most situations.
       */
      pooled: NGN.private(NGN.coalesce(cfg.poolconnections, true)),
      _originalPooling: NGN.private(NGN.coalesce(cfg.poolconnections, true)),

      _connstring: NGN.get(() => {
        return this.connectionString.replace(/\:\*{1,100}/gi, ':' + this._credential) // eslint-disable-line
      }),

      raw: NGN.privateconst(require('mongodb')),

      ObjectID: NGN.get(() => {
        return this.raw.ObjectID
      }),

      _client: NGN.private(null),

      _connected: NGN.private(false),

      _disconnecting: NGN.private(false),

      _db: NGN.private(null),

      /**
       * @cfg {number} [heartbeatInterval=9000]
       * The number of milliseconds to wait between heartbeat notifications.
       * Anything over 9 seconds (9000 milliseconds) may fail depending on the
       * configuration of the #host. By default, the client will disconnect
       * after 10 seconds. Using a 9 second value provides the longest duration
       * possible between heartbeats (minimizes network traffic) with the safest
       * buffer to prevent unexpected behavior.
       */
      heartbeatInterval: NGN.private(NGN.coalesce(cfg.heartbeat, 9000)),

      _heartbeat: NGN.private(null),

      _livetrack: NGN.private([]),

      _poolid: NGN.private(null),

      /**
       * @cfg {boolean} [fieldAsRecord=false]
       * When set to `true`, each field will be saved as it's own key/value
       * record in the Mongo collection.
       * @info This only applicable to NGN.DATA.Model proxies. Stores ignore this.
       */
      fieldAsRecord: NGN.privateconst(NGN.coalesce(cfg.fieldAsRecord, false))
    })

    this.heartbeatInterval = this.heartbeatInterval < 100 ? 9000 : this.heartbeatInterval

    this.pool({
      connected: () => {
        this._connected = true
      },

      disconnected: () => {
        this._connected = false

        if (this._disconnecting) {
          this._disconnecting = false
        }

        if (this.pooled) {
          if (NGN.DATA.ConnectionPool.clients.hasOwnProperty(this.poolId)) {
            NGN.DATA.ConnectionPool.unregisterClient(this.poolId, this.id)
          }
        }
      },

      livesync: {
        enabled: () => {
          this._originalPooling = this.pooled
          this.pooled = true
        },

        disabled: () => {
          this.pooled = this._originalPooling
        }
      }
    })

    if (this.autoconnect && this.pooled) {
      this.connect()
    }
  }

  /**
   * @property {MongoClient} client
   * A reference to the raw MongoDB client.
   * @private
   */
  get client () {
    if (!this._client) {
      this._client = this.raw.MongoClient
    }

    return this._client
  }

  /**
   * @property {Collection} collection
   * Returns a reference to the collection, primarily for executing
   * operations against the collection. If the collection is not
   * initialized, `null` is returned.
   * @private
   */
  get collection () {
    if (!this.connected) {
      NGN.BUS.emit('NGN.ADVISORY.WARN', 'Attempting to access MongoDB connection established while attempting to reference a collection.')
      throw new Error('No connection established.')
    }

    if (this.client && this._collection) {
      return this._db.collection(this._collection)
    } else {
      return null
    }
  }

  set collection (value) {
    let old = this._collection
    this._collection = value

    if (old !== null) {
      this.emit('collection.changed', {
        old: old,
        new: value
      })
    }
  }

  /**
   * @property {string} connectionString
   * The full connection string.
   */
  get connectionString () {
    // let creds = this._credential
    return `mongodb://${this.user}:********@${this.host}:${this.port.toString()}/${this.db}`
  }

  /**
   * @property {boolean} connected
   * Indicates a connection is established to the Mongo #host.
   */
  get connected () {
    return this._connected
  }

  get poolId () {
    if (this._poolid === null) {
      this._poolid = NGN.DATA.util.checksum(this._connstring)
    }

    return this._poolid
  }

  extendModel (BaseModel) {
    let me = this

    class ModifiedModel extends BaseModel {
      constructor (data) {
        super()

        this.addMetaField('__mongoid')

        this.dataMap = this.dataMap || {}
        this.dataMap.__mongoid = '_id'

        this.relationships.forEach((join) => {
          if (this[join] instanceof NGN.DATA.Store) {
            if (this[join].model !== ModifiedModel) {
              this[join].replaceModel(me.extendModel(this[join].model))
            }
          } else {
            me.assureModelReady(this[join])
          }
        })

        if (data) {
          this.load(data)
        }
      }
    }

    return ModifiedModel
  }

  /**
   * @method init
   * Adds a metadata field to the data storage system
   * if it is an instance of NGN.DATA.Store.
   * @private
   */
  init (store) {
    super.init(store)

    if (this.type === 'store') {
      // Update any existing records
      this.store.records.forEach((record) => {
        if (!record.hasMetaField('__mongoid')) {
          record.addMetaField('__mongoid')
          record.dataMap = record.dataMap || {}
          record.dataMap['__mongoid'] = '_id'
        }
      })

      this.store.replaceModel(this.extendModel(this.store.model))
    } else if (!this.fieldAsRecord) {
      if (!this.store.has('__mongoid')) {
        this.store.addMetaField('__mongoid')
      }

      this.store.dataMap = this.store.dataMap || {}
      this.store.dataMap['__mongoid'] = '_id'
    }
  }

  /**
   * @method createId
   * Generates a new MongoDB ObjectID from the specified ID.
   * @param {string} [id]
   * A value to convert to an ID. If this is not specified, a unique GUID
   * will be generated automatically and applied to the data when appropriate.
   * @private
   */
  createId (id = null) {
    return id !== null ? this.ObjectID(id.toString().substr(0, 12)) : new this.ObjectID()
  }

  /**
   * @method connect
   * Connect to the remote database.
   */
  connect () {
    if (this.connected) {
      return
    }

    let poolconn = false

    if (this.pooled) {
      poolconn = true

      // If a connection is already established, use it
      // instead of creating a new one.
console.log(this.poolId)
      if (NGN.DATA.ConnectionPool.hasOwnProperty(this.poolId)) {
        this._db = NGN.DATA.ConnectionPool[this.poolId]
        this._db.on('reconnect', () => this.emit('reconnected'))
        this._db.on('close', () => this.emit('disconnected'))

        this.emit('connected')

        return
      }
    }

    this.client.connect(this._connstring, {
      autoReconnect: this.autoreconnect
    }, (err, db) => {
      if (err) {
        throw err
      }

      db.on('reconnect', () => this.emit('reconnected'))
      db.on('close', () => this.emit('disconnected'))

      this._heartbeat = setInterval(() => {
        db.collection('__DNE__').find({
          login: ''
        })
        this.emit('heartbeat')
      }, this.heartbeatInterval)

      this._db = db

      if (poolconn) {
        NGN.DATA.ConnectionPool.add(this.poolId, db, (conn, done) => {
          conn.once('close', done)
          conn.close()
        })

        NGN.DATA.ConnectionPool.registerClient(this.poolId, this.id)
      }

      this.emit('connected')
    })
  }

  /**
   * @method disconnect
   * Disconnect from the database.
   */
  disconnect () {
    if (!this._connected) {
      return
    }

    this._disconnecting = true

    clearInterval(this._heartbeat)

    this.liveSyncEnabled = false

    if (!this.pooled) {
      this._db.close()
    } else {
      this.emit('disconnected')
    }
  }

  /**
   * @method preconnect
   * A method used to establish a connection if it is
   * not already established.
   * @param {function} [callback]
   * An optional callback executes after the connection is established.
   * Receives no arguments.
   * @private
   */
  preconnect (callback) {
    if (this.connected) {
      if (NGN.isFn(callback)) {
        callback()
      }

      return
    }

    this.once('connected', callback)
    this.connect()
  }

  /**
   * @method presave
   * Prepare for a save.
   * @returns {boolean}
   * Returns true if it's safe continue the save operation.
   * @private
   */
  presave (callback) {
    // If there is no connection, attempt to establish one.
    if (!this.connected) {
      this.preconnect(() => this.save.apply(this, arguments))
      return false
    }

    return true
  }

  /**
   * @method assureMongoIdAvailable
   * Makes sure a model is equipped with a MongoDB ID meta field
   * called `__mongoid`.
   * @param {NGN.DATA.Model} model
   * The data model to check/update.
   * @private
   */
  assureMongoIdAvailable (model) {
    if (model instanceof NGN.DATA.Store) {
      return NGN.BUS.emit('NGN.ADVISORY.WARN', `Cannot apply MongoID to a store: ${JSON.stringify(model.data, null, 2)}`)
    }

    // Assure a Mongo ID is available
    if (!model.hasMetaField('__mongoid')) {
      model.addMetaField('__mongoid')
      model.dataMap = model.dataMap || {}
      model.dataMap._id = '__mongoid'
    }
  }

  /**
   * @method assureMongoId
   * Makes sure a MongoDB ObjectId is assigned to a model
   * as a metadata field called `__mongoid`.
   * @param {NGN.DATA.Model} model
   * The data model to check/update.
   * @private
   */
  assureMongoId (model) {
    this.assureMongoIdAvailable(model)

    // If no ID exists, make one.
    try {
      model.__mongoid = NGN.coalesce(model.__mongoid, this.createId())
    } catch (e) {
      console.error(model.__mongoid)
      throw e
    }
  }

  /**
   * @method assureModelReady
   * Makes sure a model and all nested stores/models
   * are Mongo-ready.
   */
  assureModelReady (model) {
    this.assureMongoIdAvailable(model)

    model.relationships.forEach((join) => {
      if (model[join] instanceof NGN.DATA.Store) {
        model[join].replaceModel(this.extendModel(model[join].model))
      } else {
        this.assureModelReady(model[join])
      }
    })
  }

  /**
   * @method appendQuery
   * A helper method for appending query items to a bulk operation.
   * @param {object} query
   * The "main" query to add items to.
   * @param {object} flattenedQuery
   * The result of another flattened query
   */
  appendQuery (query, flattenedQuery) {
    Object.keys(flattenedQuery).forEach((coll) => {
      flattenedQuery[coll].forEach((result) => {
        query[coll] = query[coll] || []
        query[coll].push(result)
      })
    })
  }

  /**
   * @method flattenModelQuery
   * This is used to flatten nested models/stores (relationships) into
   * an array of query items. This should not be used directly.
   * @param {NGN.DATA.Model} model
   * The model to flatten.
   * @param {string} collection
   * The collection to persist to.
   * @param {function} callback
   * A callback to execute when the operation is complete.
   * This receives an error argument (`null` if no error occurred) and
   * a query object. The query object looks like:
   *
   * ```js
   * {
   *   collection_a: [{
   *     updateOne: {
   *       filter: {
   *         _id: ObjectId('...')
   *       },
   *       update: {...},
   *       upsert: true
   *     }
   *   }, {
   *     updateOne: {
   *       filter: {
   *         _id:  ObjectId('...')
   *       },
   *       update: {...},
   *       upsert: true
   *     }
   *   }],
   *   collection_b: [{
   *     updateOne: {
   *       filter: {
   *         _id: ObjectId('...')
   *       },
   *       update: {...},
   *       upsert: true
   *     }
   *   }]
   * }
   * ```
   * The response from this method can be used in bulkWrite
   * operations on the MongoDB.
   * @private
   */
  flattenModelQuery (model, collection, callback) {
    if (model instanceof NGN.DATA.Store) {
      NGN.BUS.emit('NGN.ADVISORY.WARN', 'Cannot flatten a store.')
      return
    }

    let data = model.data

    // Create a placeholder for results.
    let query = {}

    // Create an operations queue
    let tasks = new TaskRunner()

    // If the model has more relationships, expand them.
    if (model.relationships.length > 0) {
      // Flatten bulkWrite query for Mongo
      model.relationships.forEach((field) => {
        if (model[field] instanceof NGN.DATA.Store) {
          data[field] = []
          model[field].records.forEach((record) => {
            if (!record.proxyignore) {
              this.assureMongoId(record)

              data[field].push(record.__mongoid)
              // data[field].push(Object.defineProperty({}, field + '_id', NGN.public(record.__mongoid)))

              tasks.add((next) => this.flattenModelQuery(record, field, (err, subquery) => {
                if (err) {
                  throw err
                }

                this.appendQuery(query, subquery)

                next()
              }))
            }
          })
        } else if (!model[field].proxyignore) {
          this.assureMongoId(model[field])
          data[field] = model[field].__mongoid

          tasks.add((next) => this.flattenModelQuery(model[field], field, (err, subquery) => {
            if (err) {
              throw err
            }

            this.appendQuery(query, subquery)

            next()
          }))
        }
      })
    }

    tasks.add((next) => {
      this.assureMongoId(model)

      query[collection] = query[collection] || []
      query[collection].push({
        updateOne: {
          filter: {
            _id: model.__mongoid
          },
          update: data,
          upsert: true
        }
      })

      next()
    })

    tasks.on('complete', () => callback(null, query))
    tasks.run(true)
  }

  /**
   * @method save
   * Save data to the specified #collection.
   * @param {function} [callback]
   * An optional callback executes after the save is complete. Receives no arguments.
   * @fires save
   * Fired after the save is complete.
   */
  save (callback) {
    if (this.type === 'store') {
      // Persist all new and modified records.
      this.store.addFilter((record) => {
        return record.isNew || record.modified
      })

      // Abort if nothing requires updating
      if (this.store.recordCount === 0) {
        this.store.clearFilters()
        return this.postsave(callback)
      }

      // Run pre-save checks
      if (!this.presave(callback)) {
        this.store.clearFilters()
        return
      }

      // Setup a processing queue
      let tasks = new TaskRunner()
      let operations = []

      // Assure each record has a MongoID
      this.store.records.forEach((record) => {
        this.assureMongoId(record)

        tasks.add((next) => {
          if (this.expandRelationships) {
            this.flattenModelQuery(record, this._collection, (err, query) => {
              if (err) {
                throw err
              }

              Object.keys(query).forEach((coll) => {
                this._db.collection(coll).bulkWrite(query[coll]).then(next).catch((e) => {
                  throw e
                })
              })
            })
          } else {
            operations.push({
              updateOne: {
                filter: {
                  _id: record.__mongoid
                },
                update: record.data,
                upsert: true
              }
            })

            next()
          }
        })
      })

      tasks.add((next) => {
        if (operations.length === 0) {
          return next()
        }

        this.collection.bulkWrite(operations).then(next).catch((e) => {
          throw e
        })
      })

      tasks.on('complete', () => {
        this.store.clearFilters()
        this.postsave(callback)
      })

      tasks.run(true)
    } else {
      // Ignore the save operation if nothing has changed.
      if (!this.store.isNew && !this.store.modified) {
        return
      }

      // Run pre-save checks
      if (!this.presave(callback)) {
        return
      }
      // If the fields should be a single record,
      // save the entire model content as a single record.
      if (!this.fieldAsRecord) {
        this.assureMongoId(this.store)

        let tasks = new TaskRunner()

        // Expand nested fields if necessary
        if (this.expandRelationships && this.store.relationships.length > 0) {
          this.flattenModelQuery(this.store, this._collection, (err, query) => {
            if (err) {
              throw err
            }

            Object.keys(query).forEach((coll) => {
              tasks.add((more) => {
                this._db.collection(coll).bulkWrite(query[coll]).then(more).catch((e) => {
                  throw e
                })
              })
            })
          })
        } else {
          tasks.add((next) => {
            this.collection.bulkWrite([{
              updateOne: {
                filter: {
                  _id: this.store.__mongoid
                },
                update: this.store.data,
                upsert: true
              }
            }]).then(next).catch((e) => {
              console.error(e)
              next()
            })
          })
        }

        tasks.on('complete', () => {
          this.postsave(callback)
        })

        return tasks.run(true)
      }

      // If configured to do do,
      // Add each field as it's own MongoDB record.
      let operations = []

      Object.keys(this.store.data).forEach((key) => {
        operations.push({
          updateOne: {
            filter: {
              field: key
            },
            update: {
              field: key,
              value: this.store.data[key]
            },
            upsert: true
          }
        })
      })

      if (operations.length === 0) {
        return
      }

      this.collection.bulkWrite(operations).then(() => this.postsave(callback))
    }
  }

  /**
   * @method getModelCollections
   * Retrieve all the nested relationship fields as a single
   * deduplicated list of collections.
   */
  getModelCollections (model, prefix = null) {
    let relationships = NGN.coalesce(model.relationships, [])

    relationships.forEach((field) => {
      if (model[field] instanceof NGN.DATA.Store) {
        // Create a temporary new model instance from the store base model,
        // allowing this method to parse the fields of the instantiated model.
        let temp = new model[field].model() // eslint-disable-line
        relationships = relationships.concat(this.getModelCollections(temp, field))
        temp = null
      } else if (model[field] instanceof NGN.DATA.Entity || model[field] instanceof NGN.DATA.Model) {
        relationships = relationships.concat(this.getModelCollections(model[field], field))
      }
    })

    let response = NGN.dedupe(relationships)

    if (prefix) {
      response = response.map((field) => {
        return `${prefix}.${field}`
      })
    }

    return response
  }

  /**
   * @method prefetch
   * Prepare for a fetch.
   * @returns {boolean}
   * Returns true if it's safe continue the fetch operation.
   * @private
   */
  prefetch (callback) {
    // If there is no connection, attempt to establish one.
    if (!this.connected) {
      this.preconnect(() => {
        this.fetch.apply(this, arguments)
      })

      return false
    }

    return true
  }

  /**
   * @method fetch
   * Automatically populates the store/record with the full set of
   * data from the collection.
   * @param {object} [filter]
   * An optiona filter. This is a query filter passed directly to MongoDB.
   * @param {function} [callback]
   * An optional callback executes after the fetch and parse is complete. Receives no arguments.
   * @fires fetch
   * Fired after the fetch and parse is complete.
   */
  fetch (filter, callback) {
    if (NGN.isFn(filter)) {
      callback = filter
      filter = {}
    }

    if (this.type === 'store') {
      // Persist all new and modified records.
      this.store.addFilter((record) => {
        return record.isNew || record.modified
      })

      // Abort if nothing requires updating
      if (this.store.recordCount === 0) {
        this.store.clearFilters()
        return this.postfetch(callback)
      }

      // Run pre-save checks
      if (!this.prefetch(callback)) {
        this.store.clearFilters()
        return
      }

      this.collection.find(filter).toArray().then((records) => {
        this.store.once('reload', () => {
          this.store.clearFilters()
          this.postfetch(callback)
        })

        this.store.reload(records)
      })
    } else {
      // Make sure there's something to update.
      if (!this.store.isNew && !this.store.modified) {
        return this.postfetch(callback)
      }

      // Run pre-fetch checks
      if (!this.prefetch(callback)) {
        return
      }

      this.collection.findOne(NGN.coalesce(filter, {}))
        .then((doc) => {
          let tasks = new TaskRunner()

          if (this.expandRelationships) {
            this.store.relationships.forEach((field) => {
              tasks.add((next) => {
                this.getRelatedModelData(field, this.store[field], doc[field], (data) => {
                  Object.keys(doc[field]).forEach((attr) => {
                    let value = data[doc[field][attr]] instanceof Buffer ? data[doc[field][attr]].toString() : data[doc[field][attr]]

                    if (value !== undefined) {
                      doc[field][attr] = value
                    } else {
                      if (attr === 'id') {
                        NGN.BUS.emit('NGN.ADVISORY.WARN', 'Fetching expanded nested MongoDB objects stored in independent collections is not supported (pseudo-cartesian join).')
                      }
                    }
                  })

                  next()
                })
              })
            })
          }

          tasks.on('complete', () => {
            this.assureModelReady(this.store)
            this.store.once('load', () => this.postfetch(callback))
            this.store.load(doc)
          })

          tasks.run(true)
        })
        .catch((e) => console.error(e))
    }
  }

  getRelatedModelData (coll, model, referenceData, callback) {
    referenceData = NGN.typeof(referenceData) === 'array' ? referenceData : [referenceData]

    this._db.collection(coll).find({
      _id: {
        $in: referenceData
      }
    }).toArray().then((docs) => {
      if (model instanceof NGN.DATA.Store) {
        model = new model.model() // eslint-disable-line
      }

      let tasks = new TaskRunner()

      docs.forEach((doc) => {
        model.relationships.forEach((field) => {
          tasks.add((next) => {
            this.getRelatedModelData(field, model[field], doc[field], (data) => {
              for (let index in doc[field]) {
                doc[field][index] = data[doc[field][index]]
              }

              next()
            })
          })
        })
      })

      tasks.on('complete', () => {
        let uniqueData = {}
        docs.forEach((doc) => {
          uniqueData[doc._id] = doc
        })

        callback(uniqueData)
      })

      tasks.run(true)
    })
  }

  /**
   * @method postop
   * A post-operation method. This is used to cleanup any connections
   * in the case #pool is `false`.
   * @private
   */
  postop (callback) {
    // Handle live tracking if live sync is enabled.
    if (this.liveSyncEnabled && this.type === 'store') {
      this._livetrack = this.store.records.map((record) => {
        return record.__mongoid
      })
    }

    // Disconnect if necessary
    if (!this.pooled && this.connected) {
      this.once('disconnected', callback)
      this.disconnect()
      return
    }

    if (NGN.isFn(callback)) {
      callback()
    }
  }

  /**
   * @method postsave
   * Adds connection pooling support to the post-save process.
   */
  postsave (callback) {
    this.postop(() => {
      super.postsave(callback)
    })
  }

  /**
   * @method postfetch
   * Adds connection pooling support to the post-fetch process.
   */
  postfetch (callback, content) {
    this.postop(() => {
      super.postfetch(callback, content)
    })
  }

  /**
   * @method destroy
   * This destroys the remote #collection.
   * @warn This is a destructive/irreversible function. Once this executes,
   * the collection data is wiped out.
   * @param {String} [collection]
   * Optionally override the collection name. By default, this is the #collection value.
   * @param {function} [callback]
   * Executed when the method is complete. No arguments are passed to this.
   * @private
   */
  destroy (collection, callback) {
    if (typeof collection === 'function') {
      callback = collection
      collection = null
    }

    // If there is no connection, attempt to establish one.
    if (!this.connected) {
      return this.preconnect(() => {
        this.destroy.apply(this, arguments)
      })
    }

    let remotecollection = this._db.collection(NGN.coalesce(collection, this._collection))

    remotecollection.drop()
      .then(() => {
        this.once('disconnected', callback)
        this.disconnect()
      })
      .catch(() => {
        this.once('disconnected', callback)
        this.disconnect()
      })
  }

  /**
   * @method updateModelRecord
   * A private helper method for persisting a record via upsert.
   * @param {Object} change
   * The change event delivered by the NGN.DATA.Model update event.
   * @private
   */
  upsertModelRecord (change) {
    let field = this.store.getDataField(change.field)
    let value = change.new

    if (change.action === 'create') {
      if (!field.required && change.new === null) {
        return this.postop()
      }

      value = NGN.coalesce(change.new, this.store[change.field], field.required === true ? field.default : null)
    } else if (change.join) {
      value = change.originalEvent.record.data
    }

    this.preconnect(() => {
      this.collection.findOneAndUpdate({
        field: change.field.split('.')[0]
      }, {
        field: change.field.split('.')[0],
        value: value
      }, {
        upsert: true
      }).then(() => {
        this.postop(() => {
          this.emit('live.' + change.action, change)
          this.store.emit('live.' + change.action, change)
        })
      }).catch((e) => console.error(e))
    })
  }

  /**
   * @method createModelRecord
   * A private helper method for persisting new NGN.DATA.Model records via upsert.
   * @private
   */
  createModelRecord (record) {
    if (this.proxy.fieldAsRecord) {
      this.proxy.upsertModelRecord(record)
    } else {
      this.proxy.upsertStoreRecord(this.proxy.store, 'create')
    }
  }

  /**
   * @method updateModelRecord
   * A private helper method for persisting NGN.DATA.Model record modifications
   * via upsert.
   * @private
   */
  updateModelRecord (change) {
    if (change.new === change.old) {
      return
    }

    if (this.proxy.fieldAsRecord) {
      this.proxy.upsertModelRecord(change)
    } else {
      this.proxy.upsertStoreRecord(this.proxy.store, 'update')
    }
  }

  /**
   * @method deleteModelRecord
   * Removes a model-driven record based on a specified key.
   * @param {Object} change
   * The change event delivered by the NGN.DATA.Model `field.delete` event.
   * @private
   */
  removeModelRecord (change) {
    this.preconnect(() => {
      this.collection.findOneAndDelete({
        field: change.field
      }).then(() => {
        this.postop(() => {
          this.emit('live.delete', change)
          this.store.emit('live.delete', change)
        })
      })
    })
  }

  /**
   * @method deleteModelRecord
   * Removed a model-driven record from Mongo.
   * @param {Object} change
   * The change event delivered by the NGN.DATA.Model `field.delete` event.
   * @private
   */
  deleteModelRecord (change) {
    this.proxy.removeModelRecord(change)
  }

  /**
   * @method updateStoreRecord
   * A private helper method for persisting a record upsert.
   * @returns {function}
   * Returns an event handler that accepts a `record` object from the
   * NGN.DATA.Model instance (#store).
   * @private
   */
  upsertStoreRecord (record, eventName) {
    if (record.proxyignore) {
      return
    }

    this.preconnect(() => {
      this.assureMongoId(record)

      let tasks = new TaskRunner()
      let data = record.data

      if (this.expandRelationships && record.relationships.length > 0) {
        let query = {}
        record.relationships.forEach(field => {
          tasks.add((next) => {
            delete data[field]

            this.flattenModelQuery(record, field, (err, subquery) => {
              if (err) {
                throw err
              }

              this.appendQuery(query, subquery)

              next()
            })
          })
        })

        tasks.add(() => {
          query[this._collection] = query[this._collection] || []
          query[this._collection].push({
            updateOne: {
              filter: {
                _id: record.__mongoid
              },
              update: data,
              upsert: true
            }
          })
        })

        Object.keys(query).forEach((coll) => {
          tasks.add((next) => {
            this._db.collection(coll).bulkWrite(query[coll]).then(next).catch((e) => {
              console.error(e)
              next()
            })
          })
        })
      } else {
        tasks.add((next) => {
          this.collection.findOneAndUpdate({
            _id: record.__mongoid
          }, record.data, {
            upsert: true
          }).then(next).catch((e) => {
            console.error(e)
            next()
          })
        })
      }

      tasks.on('complete', () => {
        this.postop(() => {
          if (this.liveSyncEnabled) {
            this.emit(`live.${eventName}`, record)
            this.store.emit(`live.${eventName}`, record)
          }
        })
      })

      tasks.run(true)
    })
  }

  /**
   * @method createStoreRecord
   * A private helper method for persisting a record upsert.
   * @param {NGN.DATA.Model} record
   * The model of the record to update.
   * @private
   */
  createStoreRecord (record) {
    this.proxy.upsertStoreRecord(record, 'create')
  }

  /**
   * @method updateStoreRecord
   * A private helper method for persisting a record upsert.
   * @param {NGN.DATA.Model} record
   * The model of the record to create.
   * @private
   */
  updateStoreRecord (record, change) {
    if (!record.hasMetaField(change.field)) {
      this.proxy.upsertStoreRecord(record, 'update')
    }
  }

  /**
   * @method removeStoreRecord
   * Removes a store-driven record based on a specified key.
   * @param {NGN.DATA.Model} record
   * The model of the record to update.
   * @private
   */
  removeStoreRecord (record) {
    this.preconnect(() => {
      this.collection.findOneAndDelete({
        _id: record.__mongoid
      }).then(() => {
        this.postop(() => {
          if (this.liveSyncEnabled) {
            this.emit('live.delete', record)
            this.store.emit('live.delete', record)
          }
        })
      })
    })
  }

  /**
   * @method deleteStoreRecord
   * A private helper method for removing a record.
   * @param {NGN.DATA.Model} record
   * The model of the record to remove.
   * @private
   */
  deleteStoreRecord (record) {
    this.proxy.removeStoreRecord(record)
  }

  /**
   * @method clearStoreRecords
   * Removes a store-driven records.
   * @returns {function}
   * Returns an event handler that accepts a `record` object from the
   * NGN.DATA.Model instance (#store).
   * @private
   */
  clearAllStoreRecords () {
    this.collection.deleteMany({
      _id: {
        $in: this._livetrack
      }
    }).then(() => {
      this.postop(() => {
        if (this.liveSyncEnabled) {
          this.emit('live.delete', null)
          this.store.emit('live.delete', null)
        }
      })
    })
  }

  // Proxy the method.
  clearStoreRecords () {
    this.proxy.clearAllStoreRecords()
  }
}

global.NGNX = NGN.coalesce(global.NGNX, {DATA: {}})
global.NGNX.DATA = NGN.coalesce(global.NGNX.DATA, {})
Object.defineProperty(global.NGNX.DATA, 'MongoDBProxy', NGN.const(MongoProxy))
