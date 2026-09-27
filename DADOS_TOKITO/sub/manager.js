const fs = require('fs')
const path = require('path')
const { fork } = require('child_process')
const store = require('./store')
const runtime = require('./runtime')

const workers = new Map()
const callbacks = new Map()
const expectedExit = new Set()

const credsRegistered = number => {
  try {
    const file = path.join(store.ROOT, store.normalize(number), 'qrcode', 'creds.json')
    if (!fs.existsSync(file)) return false
    return JSON.parse(fs.readFileSync(file, 'utf8'))?.registered === true
  } catch {
    return false
  }
}

const sendCallback = (number, name, ...args) => {
  const cb = callbacks.get(store.normalize(number))?.[name]
  if (typeof cb === 'function') {
    Promise.resolve(cb(...args)).catch(() => {})
  }
}

const start = (number, handlers = {}) => {
  const id = store.normalize(number)
  const sub = store.get(id)
  if (!sub) throw new Error('SUB_NAO_ENCONTRADO')

  const exp = new Date(sub.expiresAt || 0).getTime()
  if (!Number.isFinite(exp) || exp <= Date.now()) {
    store.update(id, { status: 'expirado' })
    throw new Error('SUB_EXPIRADO')
  }

  if (workers.has(id)) {
    callbacks.set(id, handlers)
    return { child: workers.get(id), reused: true, registered: credsRegistered(id) }
  }

  store.ensureSubDir(id)
  callbacks.set(id, handlers)

  const child = fork(path.join(runtime.DADOS, 'connect.js'), [], {
    cwd: runtime.ROOT,
    env: {
      ...process.env,
      TOKITO_SUBBOT: '1',
      TOKITO_SUB_ID: id,
      TOKITO_SUB_NUMBER: id,
      TOKITO_SUB_OWNER: store.normalize(sub.owner),
      TOKITO_SUB_PARENT: String(process.pid)
    },
    stdio: ['ignore', 'inherit', 'inherit', 'ipc']
  })

  workers.set(id, child)
  store.update(id, { status: 'iniciando', pid: child.pid || null })

  child.on('message', message => {
    const type = String(message?.type || '')

    if (type === 'pairing-code') {
      store.update(id, { status: 'aguardando_conexao', lastPairingAt: new Date().toISOString() })
      sendCallback(id, 'onCode', String(message.code || ''), store.get(id))
      return
    }

    if (type === 'online') {
      const updated = store.update(id, {
        status: 'online',
        connectedAt: new Date().toISOString(),
        pid: child.pid || null
      })
      sendCallback(id, 'onOnline', updated)
      return
    }

    if (type === 'logged-out') {
      const updated = store.update(id, { status: 'desconectado', pid: null })
      sendCallback(id, 'onLogout', updated)
      return
    }

    if (type === 'error') {
      sendCallback(id, 'onError', new Error(String(message.message || 'Erro no Sub Bot')))
    }
  })

  child.on('exit', (code, signal) => {
    workers.delete(id)
    callbacks.delete(id)
    const esperado = expectedExit.delete(id)
    const atual = store.get(id)

    if (atual && atual.status !== 'expirado' && atual.status !== 'removido') {
      store.update(id, {
        status: esperado ? 'offline' : 'offline',
        pid: null,
        lastExitCode: code ?? null,
        lastExitSignal: signal ?? null,
        lastExitAt: new Date().toISOString()
      })
    }
  })

  child.on('error', error => {
    store.update(id, { status: 'erro', pid: null, lastError: String(error?.message || error) })
    sendCallback(id, 'onError', error)
  })

  return { child, reused: false, registered: credsRegistered(id) }
}

const stop = async number => {
  const id = store.normalize(number)
  const child = workers.get(id)
  if (!child) {
    const sub = store.get(id)
    if (sub && sub.status !== 'expirado') store.update(id, { status: 'offline', pid: null })
    return false
  }

  expectedExit.add(id)
  workers.delete(id)
  callbacks.delete(id)

  try { child.kill('SIGTERM') } catch {}
  store.update(id, { status: 'offline', pid: null })
  return true
}

const restart = async (number, handlers = {}) => {
  await stop(number)
  await new Promise(resolve => setTimeout(resolve, 700))
  return start(number, handlers)
}

const remove = async number => {
  const id = store.normalize(number)
  await stop(id)
  return store.remove(id)
}

const restore = async () => {
  store.expire()
  let started = 0
  for (const sub of store.active()) {
    if (!credsRegistered(sub.number)) continue
    if (workers.has(sub.number)) continue
    try {
      start(sub.number)
      started++
    } catch {}
  }
  return started
}

const stats = () => {
  store.expire()
  const items = store.list()
  return {
    total: items.length,
    online: items.filter(x => x.status === 'online').length,
    offline: items.filter(x => ['offline', 'desconectado', 'erro'].includes(x.status)).length,
    running: workers.size
  }
}

module.exports = { workers, start, stop, restart, remove, restore, stats, credsRegistered }