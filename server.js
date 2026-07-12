// ============================================================
// WEN LAMBO — online relay server (single file, zero deps but `ws`)
//   node server.js           # listens on :8080
//   PORT=3000 node server.js
//
//  • shards players into rooms of <=500 (SHARD_CAP)
//  • relays positions at whatever rate clients send (~15Hz)
//  • authoritative-lite: trusts client positions (arcade game, no anti-cheat)
//  • persists per-player progress by token to ./saves.json
//  • teams (<=4), leaderboard, profanity-filtered chat & names
//  • police role capped at POLICE_PER_SHARD (default 50) per shard
// ============================================================
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
let WebSocketServer;
try { WebSocketServer = require('ws').Server; }
catch (e) { console.error('\n  Missing dependency. Run:  npm install ws\n'); process.exit(1); }

const PORT = process.env.PORT || 8080;
const SHARD_CAP = +(process.env.SHARD_CAP || 500);
const POLICE_PER_SHARD = +(process.env.POLICE || 50);
const SAVE_FILE = path.join(__dirname, 'saves.json');
const TICK_MS = 66;                    // broadcast cadence (~15 Hz)
const REAP_MS = 45000;                 // drop a socket only after 45s of silence
const DAY_LEN = 560;                   // seconds per in-game day (matches the client)

// ---- always-running shared world (one clock + one sky for everyone) ----
// dayT is derived from wall-clock time so it keeps advancing even with nobody
// online, and survives restarts (tied to absolute time, not process uptime).
function worldDayT() { return ((Date.now() / 1000) / DAY_LEN) % 1; }
const WEATHER_KINDS = ['clear', 'clear', 'overcast', 'rain', 'clear', 'storm', 'overcast', 'rain'];
let worldWeather = 'clear';
let weatherHold = 120;                 // seconds until the next weather change
function stepWeather(dtSec) {
  weatherHold -= dtSec;
  if (weatherHold <= 0) {
    weatherHold = 70 + Math.random() * 110;
    const options = WEATHER_KINDS.filter(k => k !== worldWeather);
    worldWeather = options[(Math.random() * options.length) | 0];
  }
}
function worldState() { return { t: 'world', time: +worldDayT().toFixed(4), weather: worldWeather }; }

// ---- profanity (mirror of the client filter) ----
const BAD = ['fuck', 'shit', 'cunt', 'bitch', 'bastard', 'dick', 'piss', 'cock', 'pussy', 'slut', 'whore', 'nigger', 'nigga', 'faggot', 'fag', 'retard', 'rape', 'nazi', 'wank', 'twat', 'bollock', 'arse', 'kkk', 'coon', 'spic', 'chink', 'kike'];
const norm = s => (s || '').toLowerCase().replace(/[1|!]/g, 'i').replace(/3/g, 'e').replace(/[4@]/g, 'a').replace(/0/g, 'o').replace(/[5$]/g, 's').replace(/7/g, 't').replace(/[^a-z]/g, '');
const isClean = s => { const n = norm(s); return !BAD.some(w => n.includes(w)); };
const cleanChat = s => { let o = String(s); for (const w of BAD) o = o.replace(new RegExp(w.split('').join('[\\W_]*'), 'ig'), m => '*'.repeat(m.length)); return o; };

// ---- persistence ----
let saves = {};
try { saves = JSON.parse(fs.readFileSync(SAVE_FILE, 'utf8')); } catch (e) { saves = {}; }
let saveDirty = false;
const PERSIST_KEYS = ['cash', 'look', 'name', 'ownedHomes', 'ownedCars', 'ownedBoats', 'furniture', 'bedPos', 'explored', 'weapons', 'ammo', 'wardrobe', 'missionIdx', 'lamboOwned', 'arcadeBonus', 'stats'];
function persist(token, save, pos) {
  if (!token) return;
  const cur = saves[token] || {};
  if (save) for (const k of PERSIST_KEYS) if (save[k] !== undefined) cur[k] = save[k];
  // remember last OUTDOOR world position so a returning player resumes there
  if (pos && typeof pos.x === 'number' && typeof pos.y === 'number') { cur._x = pos.x; cur._y = pos.y; }
  // heal the old 99M dev wallet so it never persists or tops the board
  if (typeof cur.cash === 'number' && cur.cash > 50000000) cur.cash = 5000;
  cur._ts = Date.now();
  saves[token] = cur;
  saveDirty = true;
}
setInterval(() => { if (saveDirty) { saveDirty = false; fs.writeFile(SAVE_FILE, JSON.stringify(saves), () => { }); } }, 5000);

// ---- shards ----
let nextId = 1;
const shards = [];   // each: { id, clients:Map(id->client), teams:Map(name->team) }
function newShard() { const s = { id: shards.length, clients: new Map(), teams: new Map(), police: 0 }; shards.push(s); return s; }
function pickShard() {
  for (const s of shards) if (s.clients.size < SHARD_CAP) return s;
  return newShard();
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    const total = shards.reduce((a, s) => a + s.clients.size, 0);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, players: total, shards: shards.length, cap: SHARD_CAP }));
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('WEN LAMBO relay up. players=' + shards.reduce((a, s) => a + s.clients.size, 0));
});
const wss = new WebSocketServer({ server });

function send(ws, o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function broadcast(shard, o, exceptId) { const m = JSON.stringify(o); for (const [id, c] of shard.clients) if (id !== exceptId && c.ws.readyState === 1) c.ws.send(m); }

wss.on('connection', (ws) => {
  const client = { id: nextId++, ws, shard: null, name: 'PLAYER', token: null, x: 5440, y: 7680, a: 0, car: null, inside: null, hp: 100, dead: false, wanted: 0, look: {}, team: null, role: 'civ', god: false, kills: 0, deaths: 0, ping: 0, cash: 0, last: Date.now(), lastX: null, lastY: null };

  ws.on('message', (buf) => {
    let m; try { m = JSON.parse(buf); } catch (e) { return; }
    client.last = Date.now();
    switch (m.t) {
      case 'join': {
        // name must be clean + unique-ish
        let name = String(m.name || 'PLAYER').slice(0, 16).replace(/[^\x20-\x7E]/g, '') || 'PLAYER';
        if (!isClean(name)) { send(ws, { t: 'nametaken' }); name = 'PLAYER' + (client.id % 1000); }
        client.name = name;
        client.token = String(m.token || '').slice(0, 40);
        client.sid = String(m.sid || '').slice(0, 20);
        client.look = m.look || {};
        client.god = !!m.god;   // host/tester flag (client-declared; fine for a fun server)
        // ONE session per TAB: kick a stale connection only when it's the SAME
        // tab reconnecting (same token AND same sid). Two tabs / two machines
        // sharing a token now coexist instead of silently kicking each other —
        // that kick was the "my friend just vanished" bug.
        if (client.token) {
          for (const s of shards) {
            for (const [id, c] of [...s.clients]) {
              if (c !== client && c.token === client.token && (!client.sid || !c.sid || c.sid === client.sid)) {
                c.superseded = true;                 // don't let its close handler overwrite fresh save
                s.clients.delete(id);
                if (c.role === 'police') s.police = Math.max(0, s.police - 1);
                try { c.ws.close(); } catch (e) { }
                broadcast(s, { t: 'leave', id });
              }
            }
          }
        }
        // police role request, capped per shard
        const shard = pickShard();
        client.shard = shard;
        if (m.role === 'police' && shard.police < POLICE_PER_SHARD) { client.role = 'police'; shard.police++; }
        shard.clients.set(client.id, client);
        const save = saves[client.token];
        // returning player → resume at last outdoor spot; otherwise scatter on a street
        if (save && typeof save._x === 'number' && typeof save._y === 'number') {
          client.x = save._x; client.y = save._y;
          client.lastX = save._x; client.lastY = save._y;
        } else {
          const sp = SPAWNS[(Math.random() * SPAWNS.length) | 0];
          client.x = sp[0] + (Math.random() * 120 - 60);
          client.y = sp[1] + (Math.random() * 120 - 60);
        }
        send(ws, { t: 'welcome', id: client.id, count: shard.clients.size, role: client.role, spawn: [client.x, client.y], save: save || { empty: true }, time: +worldDayT().toFixed(4), weather: worldWeather });
        break;
      }
      case 'pos': {
        client.x = m.x; client.y = m.y; client.a = m.a; client.car = m.car;
        client.inside = m.inside || null; client.hp = m.hp; client.dead = m.dead; client.wanted = m.wanted || 0;
        client.w = m.w || null; client.roof = m.roof || null;
        // remember the last OUTDOOR spot (indoors sends room coords, not world)
        if (!client.inside && !client.roof) { client.lastX = m.x; client.lastY = m.y; }
        break;
      }
      case 'shot': {
        // cosmetic gunfire tracer — relay to everyone nearby in the shard
        if (!client.shard) break;
        const payload = JSON.stringify({ t: 'shot', x: m.x | 0, y: m.y | 0, a: +m.a || 0, w: String(m.w || 'pistol').slice(0, 12) });
        for (const [cid, c] of client.shard.clients) {
          if (cid === client.id || c.ws.readyState !== 1) continue;
          if (Math.abs(c.x - m.x) > 1700 || Math.abs(c.y - m.y) > 1700) continue;
          c.ws.send(payload);
        }
        break;
      }
      case 'hit': {
        if (!client.shard) break;
        const tgt = client.shard.clients.get(m.target);
        if (!tgt || tgt.god) break;
        // police can't damage civilians/police; players can't hit their own team
        if (client.role === 'police' && tgt.role !== 'outlaw') break;
        if (client.team && tgt.team && client.team === tgt.team) break;
        send(tgt.ws, { t: 'hit', dmg: Math.min(60, m.dmg | 0), x: m.x, y: m.y, from: client.name });
        if (tgt.hp - m.dmg <= 0) { client.kills++; tgt.deaths++; }
        break;
      }
      case 'chat': {
        if (!client.shard) break;
        broadcast(client.shard, { t: 'chat', name: client.name, text: cleanChat(String(m.text).slice(0, 120)), team: client.team }, null);
        break;
      }
      case 'teamcreate': {
        if (!client.shard) break;
        let nm = String(m.name || '').slice(0, 16);
        if (!isClean(nm) || client.shard.teams.has(nm)) { send(ws, { t: 'nametaken' }); break; }
        const team = { name: nm, owner: client.id, members: [client.id] };
        client.shard.teams.set(nm, team);
        client.team = nm;
        sendTeam(client.shard, team);
        break;
      }
      case 'teaminvite': {
        if (!client.shard || !client.team) break;
        const tgt = client.shard.clients.get(m.target);
        const team = client.shard.teams.get(client.team);
        if (tgt && team && team.members.length < 4) send(tgt.ws, { t: 'teaminvite', from: client.name, team: client.team, ownerId: team.owner });
        break;
      }
      case 'teamaccept': {
        if (!client.shard) break;
        const owner = client.shard.clients.get(m.owner);
        if (!owner || !owner.team) break;
        const team = client.shard.teams.get(owner.team);
        if (team && team.members.length < 4 && !team.members.includes(client.id)) {
          team.members.push(client.id);
          client.team = team.name;
          sendTeam(client.shard, team);
        }
        break;
      }
      // ---- CARNAGE TV online co-op lobbies (arcade relay) ----
      case 'arc_host': {
        if (!client.shard) break;
        arcLeave(client);                              // close anything he was in
        if (!client.shard.arc) client.shard.arc = new Map();
        client.shard.arc.set(client.id, { id: client.id, host: client, members: new Map(), state: 'open' });
        client.arcLobby = client.id; client.arcHost = true;
        send(ws, { t: 'arc_hosted', lobbyId: client.id });
        break;
      }
      case 'arc_list': {
        const rows = [];
        if (client.shard && client.shard.arc) {
          for (const L of client.shard.arc.values()) {
            if (L.state === 'open' && L.host.ws.readyState === 1) rows.push({ id: L.id, host: L.host.name, n: L.members.size + 1 });
          }
        }
        send(ws, { t: 'arc_lobbies', rows });
        break;
      }
      case 'arc_join': {
        const L = client.shard && client.shard.arc && client.shard.arc.get(m.lobbyId);
        if (!L || L.state !== 'open' || L.members.size >= 3 || client.arcLobby) { send(ws, { t: 'arc_deny', why: !L ? 'gone' : L.state !== 'open' ? 'playing' : 'full' }); break; }
        L.members.set(client.id, client);
        client.arcLobby = L.id; client.arcHost = false;
        send(L.host.ws, { t: 'arc_join', id: client.id, name: client.name, look: client.look });
        send(ws, { t: 'arc_joined', lobbyId: L.id, host: L.host.name });
        break;
      }
      case 'arc_leave': { arcLeave(client); break; }
      case 'arc_input': {
        const L = client.shard && client.shard.arc && client.shard.arc.get(client.arcLobby);
        if (L && !client.arcHost && L.host.ws.readyState === 1) send(L.host.ws, { t: 'arc_input', id: client.id, i: m.i });
        break;
      }
      case 'arc_state': {
        const L = client.shard && client.shard.arc && client.shard.arc.get(client.arcLobby);
        if (L && client.arcHost) {
          const s = JSON.stringify({ t: 'arc_state', s: m.s });
          for (const c of L.members.values()) if (c.ws.readyState === 1) c.ws.send(s);
        }
        break;
      }
      case 'arc_open': {
        // host flips the lobby between joinable (in the lobby screen) and closed (mid-run)
        const L = client.shard && client.shard.arc && client.shard.arc.get(client.arcLobby);
        if (L && client.arcHost) { L.state = m.open ? 'open' : 'playing'; if (m.start) for (const c of L.members.values()) send(c.ws, { t: 'arc_start' }); }
        break;
      }
      case 'save': {
        persist(client.token, m.save, client.lastX != null ? { x: client.lastX, y: client.lastY } : null);
        client.cash = (m.save && m.save.cash) || client.cash;
        if (m.save && m.save.look) client.look = m.save.look;   // wardrobe changes go live
        break;
      }
      case 'leaderboard': { send(ws, { t: 'leaderboard', rows: leaderboardFor(client.shard) }); break; }
      case 'role': {
        if (m.role === 'outlaw') client.role = 'outlaw';   // opt into being huntable by police
        break;
      }
      case 'ping': { send(ws, { t: 'pong' }); break; }   // keepalive (client.last already refreshed above)
    }
  });

  // protocol-level ping/pong so proxies (Render/CF) don't idle us out, and so we
  // can detect half-open sockets. The browser answers pings automatically.
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; client.last = Date.now(); if (ws._pingAt) client.ping = Date.now() - ws._pingAt; });

  ws.on('close', () => {
    // a superseded ghost was already cleaned up on the new join — and must NOT
    // write its stale cash/position over the fresh session.
    if (client.superseded) return;
    arcLeave(client);
    const s = client.shard;
    if (s) {
      s.clients.delete(client.id);
      if (client.role === 'police') s.police = Math.max(0, s.police - 1);
      if (client.team) {
        const team = s.teams.get(client.team);
        if (team) { team.members = team.members.filter(i => i !== client.id); if (!team.members.length) s.teams.delete(client.team); else sendTeam(s, team); }
      }
      broadcast(s, { t: 'leave', id: client.id });
    }
    if (client.token) persist(client.token, { cash: client.cash }, client.lastX != null ? { x: client.lastX, y: client.lastY } : null);
  });
});

// leave whatever arcade lobby a client hosts or sits in, notifying the rest
function arcLeave(client) {
  const s = client.shard;
  if (!s || !s.arc || !client.arcLobby) { client.arcLobby = null; client.arcHost = false; return; }
  const L = s.arc.get(client.arcLobby);
  client.arcLobby = null;
  const wasHost = client.arcHost;
  client.arcHost = false;
  if (!L) return;
  if (wasHost) {
    for (const c of L.members.values()) { c.arcLobby = null; c.arcHost = false; send(c.ws, { t: 'arc_closed' }); }
    s.arc.delete(L.id);
  } else {
    L.members.delete(client.id);
    if (L.host.ws.readyState === 1) send(L.host.ws, { t: 'arc_left', id: client.id });
  }
}

function sendTeam(shard, team) {
  const members = team.members.map(id => { const c = shard.clients.get(id); return c ? { id, name: c.name } : null; }).filter(Boolean);
  const payload = { t: 'team', team: { name: team.name, owner: team.owner, members } };
  for (const id of team.members) { const c = shard.clients.get(id); if (c) send(c.ws, payload); }
}
function leaderboardFor(shard) {
  if (!shard) return [];
  return [...shard.clients.values()]
    .map(c => ({ name: c.name, kills: c.kills, deaths: c.deaths || 0, cash: c.cash, ping: c.ping || 0, team: c.team, role: c.role }))
    // stable order: kills desc, then cash desc, then name — so it doesn't shuffle
    .sort((a, b) => b.kills - a.kills || b.cash - a.cash || (a.name < b.name ? -1 : a.name > b.name ? 1 : 0))
    .slice(0, 20);
}

// broadcast world state to each shard
setInterval(() => {
  for (const shard of shards) {
    if (!shard.clients.size) continue;
    // For big shards, send an interest-managed slice per player (nearby only).
    const all = [...shard.clients.values()];
    const compact = all.map(c => ({ id: c.id, x: c.x, y: c.y, a: c.a, car: c.car, look: c.look, name: c.name, hp: c.hp, dead: c.dead, team: c.team, inside: c.inside, role: c.role, w: c.w || null, roof: c.roof || null }));
    for (const c of all) {
      // send only players within ~2500px (plus everyone if the shard is small)
      let players;
      if (compact.length <= 60) players = compact;
      else players = compact.filter(o => o.id === c.id || (Math.abs(o.x - c.x) < 2500 && Math.abs(o.y - c.y) < 2500));
      send(c.ws, { t: 'state', players, count: shard.clients.size, full: compact.length <= 60 });
    }
  }
}, TICK_MS);

// push leaderboard + the shared world clock/sky every 5s
setInterval(() => {
  stepWeather(5);
  const world = worldState();
  for (const shard of shards) {
    const rows = leaderboardFor(shard);
    for (const c of shard.clients.values()) { send(c.ws, { t: 'leaderboard', rows }); send(c.ws, world); }
  }
}, 5000);

// WebSocket keepalive — ping every 20s. Proxies that would otherwise close an
// "idle" socket see traffic; genuinely dead sockets fail to pong and get reaped.
setInterval(() => {
  for (const shard of shards) for (const [id, c] of shard.clients) {
    if (c.ws.readyState !== 1) continue;
    if (c.ws.isAlive === false) { try { c.ws.terminate(); } catch (e) { } continue; }
    c.ws.isAlive = false;
    c.ws._pingAt = Date.now();
    try { c.ws.ping(); } catch (e) { }
  }
}, 20000);

// reap silent sockets (no pos/ping/pong for REAP_MS)
setInterval(() => { const now = Date.now(); for (const shard of shards) for (const [id, c] of shard.clients) if (now - c.last > REAP_MS) { try { c.ws.terminate(); } catch (e) { } } }, 15000);

// street spawn points (roads/plazas across the map, in px). Players scatter.
const SPAWNS = [
  [5440, 7680], [3400, 2800], [4600, 3400], [1800, 3800], [2600, 5200], [7000, 4600],
  [3000, 7000], [5000, 6000], [4200, 4800], [1600, 6200], [6000, 8000], [2400, 2400],
  [4800, 2000], [3600, 6400], [5600, 4200], [2000, 5000], [6400, 6800], [3200, 4000],
];

newShard();
server.listen(PORT, () => {
  console.log('WEN LAMBO relay listening on :' + PORT + '  (shard cap ' + SHARD_CAP + ', police/shard ' + POLICE_PER_SHARD + ')');
});
