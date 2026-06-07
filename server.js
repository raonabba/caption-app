const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

// rooms: { roomCode: { sender: ws | null, displays: Set<ws> } }
const rooms = new Map();

function getOrCreateRoom(code) {
  if (!rooms.has(code)) {
    rooms.set(code, { sender: null, displays: new Set() });
  }
  return rooms.get(code);
}

function cleanRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  if (!room.sender && room.displays.size === 0) {
    rooms.delete(code);
  }
}

function broadcast(room, data) {
  const msg = JSON.stringify(data);
  room.displays.forEach(ws => {
    if (ws.readyState === ws.OPEN) ws.send(msg);
  });
}

wss.on('connection', (ws) => {
  let currentRoom = null;
  let currentRole = null; // 'sender' | 'display'

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // { type: 'join', role: 'sender'|'display', room: '1234' }
    if (msg.type === 'join') {
      const code = String(msg.room).trim();
      const role = msg.role;
      if (!code || !['sender', 'display'].includes(role)) return;

      currentRoom = code;
      currentRole = role;
      const room = getOrCreateRoom(code);

      if (role === 'sender') {
        // 기존 sender가 있으면 내보내기
        if (room.sender && room.sender.readyState === room.sender.OPEN) {
          room.sender.send(JSON.stringify({ type: 'kicked' }));
          room.sender.close();
        }
        room.sender = ws;
        ws.send(JSON.stringify({ type: 'joined', role, room: code, displays: room.displays.size }));
      } else {
        room.displays.add(ws);
        ws.send(JSON.stringify({ type: 'joined', role, room: code }));
        // sender에게 display 접속 알림
        if (room.sender && room.sender.readyState === room.sender.OPEN) {
          room.sender.send(JSON.stringify({ type: 'display_joined', count: room.displays.size }));
        }
      }
      return;
    }

    // { type: 'text', text: '...', interim: true|false }
    if (msg.type === 'text' && currentRole === 'sender') {
      const room = rooms.get(currentRoom);
      if (!room) return;
      broadcast(room, { type: 'text', text: msg.text, interim: !!msg.interim });
    }

    // { type: 'clear' }
    if (msg.type === 'clear' && currentRole === 'sender') {
      const room = rooms.get(currentRoom);
      if (!room) return;
      broadcast(room, { type: 'clear' });
    }
  });

  ws.on('close', () => {
    if (!currentRoom) return;
    const room = rooms.get(currentRoom);
    if (!room) return;

    if (currentRole === 'sender') {
      room.sender = null;
      broadcast(room, { type: 'sender_disconnected' });
    } else {
      room.displays.delete(ws);
      if (room.sender && room.sender.readyState === room.sender.OPEN) {
        room.sender.send(JSON.stringify({ type: 'display_left', count: room.displays.size }));
      }
    }
    cleanRoom(currentRoom);
  });
});

console.log(`VOIX WebSocket server running on port ${PORT}`);
