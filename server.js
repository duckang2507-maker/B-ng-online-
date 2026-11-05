const http = require('http'), fs = require('fs'), path = require('path');
const { WebSocketServer } = require('ws');

const html = fs.readFileSync(path.join(__dirname, 'index.html'));
const server = http.createServer((req, res) => {
  if (req.url === '/health') { res.end('ok'); return; }
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
  res.end(html);
});

const wss = new WebSocketServer({ server, maxPayload: 8192 });
const rooms = new Map(); // roomName -> Map(id -> ws)
let nextId = 1;
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

wss.on('connection', ws => {
  ws.id = nextId++; ws.room = null; ws.alive = true; ws.pres = Object.create(null);
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === 'join' && !ws.room) {
      const name = String(m.name || '').replace(/[^a-z0-9_.-]/gi, '').slice(0, 20);
      if (!name) return;
      let r = rooms.get(name);
      if (!r) { r = new Map(); rooms.set(name, r); }
      if (r.size >= 2) { send(ws, { t: 'full' }); return; }
      const others = [...r.values()].map(c => ({ id: c.id, presence: c.pres }));
      r.set(ws.id, ws); ws.room = name;
      send(ws, { t: 'joined', id: ws.id, peers: others });
      for (const c of r.values()) if (c !== ws) send(c, { t: 'join', id: ws.id });
    } else if (m.t === 'p' && ws.room) {
      const patch = m.patch;
      if (!patch || typeof patch !== 'object') return;
      for (const k in patch) { if (patch[k] === null) delete ws.pres[k]; else ws.pres[k] = patch[k]; }
      const r = rooms.get(ws.room);
      if (r) for (const c of r.values()) if (c !== ws) send(c, { t: 'p', id: ws.id, patch });
    }
  });
  ws.on('close', () => {
    const r = ws.room && rooms.get(ws.room);
    if (!r) return;
    r.delete(ws.id);
    if (!r.size) rooms.delete(ws.room);
    for (const c of r.values()) send(c, { t: 'leave', id: ws.id });
  });
});

setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); });
}, 30000);

server.listen(process.env.PORT || 3000, () => console.log('Server chạy ở cổng', process.env.PORT || 3000));
