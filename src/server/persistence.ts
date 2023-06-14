import * as Y from 'yjs'
import type { PersistenceProvider } from './y-socket-io'

export interface GenericPersistence {
  /**
   * Get all of the updates for the named document.
   */
  getUpdates(docName: string): Promise<Uint8Array[]>
  /**
   * Store a single document update.
   */
  storeUpdate(docName: string, update: Uint8Array): Promise<void>
  /**
   * Store the complete single state of a document.
   */
  flushDocument(docName: string, state: Uint8Array): Promise<void>
}

export class GenericPersistenceAdaptor implements PersistenceProvider {
  private db: GenericPersistence
  private tr: Promise<unknown>
  private trimSize: number

  constructor(db: GenericPersistence, trimSize = 500) {
    this.db = db
    this.tr = Promise.resolve()
    this.trimSize = trimSize
  }

  /**
   * Execute an transaction on a database. This will ensure that other processes are currently not writing.
   *
   * This is a private method and might change in the future.
   *
   * @todo only transact on the same room-name. Allow for concurrency of different rooms.
   */
  private _transact<T>(f: (db: GenericPersistence) => Promise<T>): Promise<T | null> {
    const currTr = this.tr
    this.tr = (async () => {
      await currTr
      let res: T | null = null
      try {
        res = await f(this.db)
      } catch (err) {
        console.warn('Error during PersistenceProvider transaction', err)
      }
      return res
    })()
    return this.tr as Promise<T | null>
  }

  /**
   * Consolidate all doc updates into a single document.
   */
  async flushDocument(docName: string) {
    // using trimSize=0 causes all updates to be consolidated into a single state
    await this.getYDoc(docName, 0)
  }

  /**
   * Get the current state of a document by name.
   */
  async getYDoc(docName: string, trimSize = this.trimSize): Promise<Y.Doc> {
    const dbdoc = await this._transact<Y.Doc>(async db => {
      const updates = await db.getUpdates(docName)
      const ydoc = new Y.Doc()
      ydoc.transact(() => {
        updates.forEach(update => {
          Y.applyUpdate(ydoc, update)
        })
      })
      if (updates.length > trimSize) {
        await db.flushDocument(docName, Y.encodeStateAsUpdate(ydoc))
      }
      return ydoc
    })
    if (dbdoc) {
      return dbdoc
    }
    return new Y.Doc()
  }

  /**
   * Store a single document update.
   */
  async storeUpdate(docName: string, update: Uint8Array) {
    await this._transact(db => db.storeUpdate(docName, update))
  }
}
