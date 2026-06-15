'use strict';

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const multer = require('multer');
const crypto = require('crypto');
const { v4: uuidv4 } = require('uuid');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 5e6 });

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true, limit: '2mb' }));

const PORT = process.env.PORT || 3000;

// ---------- In-memory storage ----------
const users = new Map();
const sessions = new Map();
const chats = new Map(); // chatId -> { id, type: 'global'|'private'|'group', name, members, messages[], pinnedId, admins }
const onlineSockets = new Map();
const unreadCounts = new Map(); // userId -> { chatId -> count }

let adminId = null;

const MAX_GLOBAL_HISTORY = 200;
const MAX_PRIVATE_HISTORY = 100;
const MAX_GROUP_HISTORY = 300;
const MAX_AVATAR_BASE64_LEN = 50 * 1024;
const MAX_FILE_BASE64_LEN = 3 * 1024 * 1024;
const MESSAGE_MAX_LEN = 1000;
const EDIT_WINDOW_MS = 48 * 60 * 60 * 1000;

// ---------- Theme presets ----------
const THEMES = {
  dark: { name: 'Telegram Dark', bg: '#0e1621', sidebar: '#17212b', bubbleOwn: '#2b5278', bubbleOther: '#182533', accent: '#3390ec', text: '#fff', muted: '#7f8c8d', border: '#242f3d', header: '#17212b' },
  light: { name: 'Telegram Light', bg: '#ffffff', sidebar: '#f5f5f5', bubbleOwn: '#effdde', bubbleOther: '#ffffff', accent: '#3390ec', text: '#000', muted: '#707579', border: '#dfe1e5', header: '#ffffff' },
  midnight: { name: 'Midnight', bg: '#0d0d1a', sidebar: '#16162a', bubbleOwn: '#4b2d78', bubbleOther: '#1f1f3a', accent: '#8b5cf6', text: '#e6e6ff', muted: '#8b8bb0', border: '#2a2a4a', header: '#16162a' },
  ocean: { name: 'Ocean', bg: '#0a1f2e', sidebar: '#0f2d3f', bubbleOwn: '#1b6b93', bubbleOther: '#102a3d', accent: '#4fc0d0', text: '#e0f7fa', muted: '#82b0b8', border: '#1a3c52', header: '#0f2d3f' },
  sunset: { name: 'Sunset', bg: '#1a1018', sidebar: '#2a1824', bubbleOwn: '#8b3a62', bubbleOther: '#2e1c28', accent: '#ff6b9d', text: '#fff0f5', muted: '#c49aa8', border: '#442234', header: '#2a1824' },
  matrix: { name: 'Matrix', bg: '#000', sidebar: '#081008', bubbleOwn: '#003b00', bubbleOther: '#0a1a0a', accent: '#00ff41', text: '#e8ffe8', muted: '#2a8a2a', border: '#0f3d0f', header: '#081008' },
  pink: { name: 'Sakura', bg: '#1f141c', sidebar: '#2e1d28', bubbleOwn: '#7c3e5e', bubbleOther: '#2a1b24', accent: '#ff8fb1', text: '#fff0f5', muted: '#d4a8b8', border: '#4a2e3d', header: '#2e1d28' },
  gold: { name: 'Luxury Gold', bg: '#12100e', sidebar: '#1c1814', bubbleOwn: '#5c4b1e', bubbleOther: '#221e18', accent: '#d4af37', text: '#f5efe0', muted: '#a89a7a', border: '#3a332a', header: '#1c1814' }
};

// ---------- Helpers ----------
function escapeHTML(text) { return String(text == null ? '' : text).replace(/[&<>"']/g, (m) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m])); }
function hashPassword(password, salt) { return crypto.createHmac('sha256', salt).update(password).digest('hex'); }
function generateToken() { return crypto.randomBytes(32).toString('hex'); }
function getPrivateKey(a, b) { return [a, b].sort().join('|'); }
function trimHistory(arr, limit) { if (arr.length > limit) arr.splice(0, arr.length - limit); }

function getUserPublicProfile(user) {
  const online = (onlineSockets.get(user.id) || new Set()).size > 0;
  return { id: user.id, username: user.username, avatar: user.avatarBase64 || null, about: user.about || '', theme: user.theme || 'dark', online, lastSeen: user.lastSeen || Date.now(), isAdmin: user.id === adminId };
}

function broadcastUsers() {
  io.emit('users_list', Array.from(users.values()).map(getUserPublicProfile));
}

function broadcastToUser(userId, event, payload) {
  const set = onlineSockets.get(userId);
  if (!set) return;
  set.forEach((socketId) => { const s = io.sockets.sockets.get(socketId); if (s) s.emit(event, payload); });
}

function getChat(chatId) { return chats.get(chatId); }

function ensureGlobalChat() {
  if (!chats.has('global')) {
    chats.set('global', { id: 'global', type: 'global', name: 'Global Chat', members: new Set(), messages: [], pinnedId: null });
  }
  return chats.get('global');
}

function ensurePrivateChat(userA, userB) {
  const key = getPrivateKey(userA, userB);
  if (!chats.has(key)) {
    chats.set(key, { id: key, type: 'private', name: null, members: new Set([userA, userB]), messages: [], pinnedId: null });
  }
  return chats.get(key);
}

function createGroup(name, creatorId, members) {
  const id = 'group:' + uuidv4();
  const all = new Set([creatorId, ...(members || [])]);
  const chat = { id, type: 'group', name: escapeHTML(String(name).slice(0, 40)), members: all, messages: [], pinnedId: null, admins: new Set([creatorId]) };
  chats.set(id, chat);
  return chat;
}

function isGroupAdmin(chat, userId) { return chat.admins && chat.admins.has(userId); }

function addSystemMessage(chatId, text) {
  const chat = getChat(chatId);
  if (!chat) return;
  const msg = { id: uuidv4(), type: 'system', text: escapeHTML(text), timestamp: Date.now(), deletedForAll: false };
  chat.messages.push(msg);
  trimHistory(chat.messages, chat.type === 'private' ? MAX_PRIVATE_HISTORY : (chat.type === 'group' ? MAX_GROUP_HISTORY : MAX_GLOBAL_HISTORY));
  io.emit('system_message', { chatId, ...msg });
}

function storeMessage(chat, senderId, payload) {
  const delivered = new Set();
  if (chat.type === 'global') { Array.from(onlineSockets.keys()).forEach((uid) => delivered.add(uid)); }
  else if (chat.type === 'private') { if (onlineSockets.has(senderId)) delivered.add(senderId); if (onlineSockets.has(chat.members.values().next().value === senderId ? Array.from(chat.members)[1] : Array.from(chat.members)[0])) delivered.add(Array.from(chat.members).find((id) => id !== senderId)); }
  else if (chat.type === 'group') { chat.members.forEach((uid) => { if (onlineSockets.has(uid)) delivered.add(uid); }); }
  const msg = Object.assign({ id: uuidv4(), senderId, timestamp: Date.now(), status: delivered.size > 0 ? 'delivered' : 'sent', reactions: {}, replyTo: null, editedAt: null, deletedForAll: false }, payload);
  chat.messages.push(msg);
  trimHistory(chat.messages, chat.type === 'private' ? MAX_PRIVATE_HISTORY : (chat.type === 'group' ? MAX_GROUP_HISTORY : MAX_GLOBAL_HISTORY));
  // Increment unread counts
  if (chat.type === 'global') {
    for (const uid of users.keys()) {
      if (uid !== senderId) { incUnread(uid, chat.id); }
    }
  } else if (chat.type === 'private') {
    const other = Array.from(chat.members).find((id) => id !== senderId);
    if (other) incUnread(other, chat.id);
  } else if (chat.type === 'group') {
    chat.members.forEach((uid) => { if (uid !== senderId) incUnread(uid, chat.id); });
  }
  return msg;
}
function incUnread(userId, chatId) {
  if (!unreadCounts.has(userId)) unreadCounts.set(userId, new Map());
  const m = unreadCounts.get(userId);
  m.set(chatId, (m.get(chatId) || 0) + 1);
}
function clearUnread(userId, chatId) {
  if (unreadCounts.has(userId)) {
    const m = unreadCounts.get(userId);
    m.delete(chatId);
  }
}
function getUnreadCount(userId, chatId) {
  return unreadCounts.get(userId)?.get(chatId) || 0;
}

function enrichMessage(msg) {
  if (msg.type === 'system') return Object.assign({}, msg);
  const sender = users.get(msg.senderId);
  const clone = Object.assign({}, msg);
  clone.sender = sender ? getUserPublicProfile(sender) : { id: msg.senderId, username: 'Unknown' };
  if (msg.replyTo) {
    const chat = null; // not used here; reply enriched client side via cached messages
  }
  return clone;
}

function broadcastChatMessage(chat, msg) {
  const payload = { chatId: chat.id, message: enrichMessage(msg) };
  if (chat.type === 'global') io.emit('message', payload);
  else chat.members.forEach((uid) => broadcastToUser(uid, 'message', payload));
}

function broadcastChatUpdate(chat, type, data) {
  const payload = Object.assign({ chatId: chat.id, type }, data);
  if (chat.type === 'global') io.emit('chat_update', payload);
  else chat.members.forEach((uid) => broadcastToUser(uid, 'chat_update', payload));
}

function setUserOnline(user, socket) {
  socket.data.userId = user.id;
  let set = onlineSockets.get(user.id);
  if (!set) { set = new Set(); onlineSockets.set(user.id, set); }
  set.add(socket.id);
  user.lastSeen = Date.now();
}

function setUserOffline(socket) {
  const userId = socket.data.userId;
  if (!userId) return;
  const user = users.get(userId);
  const set = onlineSockets.get(userId);
  if (set) {
    set.delete(socket.id);
    if (set.size === 0) {
      onlineSockets.delete(userId);
      if (user) { user.lastSeen = Date.now(); addSystemMessage('global', user.username + ' left'); }
    }
  }
  delete socket.data.userId;
}

function getChatName(chat, userId) {
  if (chat.type === 'global') return 'Global Chat';
  if (chat.type === 'group') return chat.name;
  const other = Array.from(chat.members).find((id) => id !== userId);
  const u = users.get(other);
  return u ? u.username : 'Unknown';
}

function getChatListForUser(userId) {
  const list = [];
  for (const chat of chats.values()) {
    if (chat.type === 'global') {
      list.push({ id: chat.id, type: 'global', name: 'Global Chat', avatar: null, lastMessage: chat.messages[chat.messages.length - 1] || null, unread: 0 });
    } else if (chat.type === 'private') {
      if (!chat.members.has(userId)) continue;
      const other = Array.from(chat.members).find((id) => id !== userId);
      const u = users.get(other);
      list.push({ id: chat.id, type: 'private', userId: other, name: u ? u.username : 'Unknown', avatar: u ? u.avatarBase64 : null, lastMessage: chat.messages[chat.messages.length - 1] || null, unread: 0 });
    } else if (chat.type === 'group') {
      if (!chat.members.has(userId)) continue;
      list.push({ id: chat.id, type: 'group', name: chat.name, avatar: null, lastMessage: chat.messages[chat.messages.length - 1] || null, unread: 0, membersCount: chat.members.size });
    }
  }
  return list;
}

function markMessagesRead(chatId, userId) {
  clearUnread(userId, chatId);
}

function getUnreadCountsForUser(userId) {
  const m = unreadCounts.get(userId);
  return m ? Object.fromEntries(m) : {};
}

// ---------- Multer uploads ----------
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 2 * 1024 * 1024 } });
app.post('/api/avatar', upload.single('avatar'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  const dataUrl = 'data:' + req.file.mimetype + ';base64,' + req.file.buffer.toString('base64');
  if (dataUrl.length > MAX_AVATAR_BASE64_LEN) return res.status(413).json({ error: 'Avatar too large' });
  res.json({ url: dataUrl });
});
app.post('/api/file', upload.single('file'), (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });
  const dataUrl = 'data:' + req.file.mimetype + ';base64,' + req.file.buffer.toString('base64');
  if (dataUrl.length > MAX_FILE_BASE64_LEN) return res.status(413).json({ error: 'File too large' });
  res.json({ url: dataUrl, name: req.file.originalname, size: req.file.size, mime: req.file.mimetype });
});

// ---------- HTML page ----------
const HTML_PAGE = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>Telegram Clone Pro</title>
<script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
<script src="/socket.io/socket.io.js"></script>
<style>
:root { --bg: #0e1621; --sidebar: #17212b; --bubble-own: #2b5278; --bubble-other: #182533; --accent: #3390ec; --text: #fff; --muted: #7f8c8d; --border: #242f3d; --header: #17212b; --shadow: rgba(0,0,0,.35); }
* { box-sizing: border-box; -webkit-tap-highlight-color: transparent; }
html, body { margin:0; height:100%; background: var(--bg); color: var(--text); font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }
#app { height:100%; }
.hidden { display: none !important; }
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(127,140,141,.4); border-radius: 4px; }
.auth-screen { height:100%; display:flex; align-items:center; justify-content:center; background: radial-gradient(circle at top, rgba(51,144,236,.15), transparent 60%), var(--bg); padding:16px; }
.auth-card { width:100%; max-width:380px; background: var(--sidebar); padding:28px; border-radius:18px; box-shadow:0 16px 48px rgba(0,0,0,.45); }
.auth-title { text-align:center; margin-bottom:20px; color: var(--accent); font-size:26px; font-weight:800; letter-spacing:-.5px; }
.auth-tabs { display:flex; margin-bottom:18px; border-bottom:1px solid var(--border); }
.auth-tabs button { flex:1; background:none; border:none; color: var(--muted); padding:12px; cursor:pointer; font-size:15px; transition:.2s; }
.auth-tabs button.active { color: var(--text); border-bottom:2px solid var(--accent); }
.tabs-bar { display:flex; border-bottom:1px solid var(--border); }
.tab-btn { flex:1; background:none; border:none; color:var(--muted); padding:12px; cursor:pointer; font-size:14px; font-weight:600; transition:.2s; border-bottom:2px solid transparent; }
.tab-btn.active { color:var(--text); border-bottom:2px solid var(--accent); }
.auth-form input { width:100%; margin-bottom:14px; padding:12px; background: var(--bg); border:1px solid var(--border); border-radius:10px; color: var(--text); outline:none; }
.auth-form input:focus { border-color: var(--accent); }
.btn-primary { width:100%; padding:12px; background: var(--accent); border:none; border-radius:10px; color:#fff; cursor:pointer; font-weight:700; transition:.2s; }
.btn-primary:hover { filter: brightness(1.1); }
.btn-secondary { width:100%; padding:12px; background: transparent; border:1px solid var(--border); border-radius:10px; color: var(--text); cursor:pointer; margin-top:10px; transition:.2s; }
.btn-secondary:hover { background: rgba(127,140,141,.12); }
.drop-zone { border:2px dashed var(--border); border-radius:14px; padding:18px; text-align:center; cursor:pointer; margin-bottom:14px; color: var(--muted); transition:.2s; }
.drop-zone.dragover { border-color: var(--accent); background: rgba(51,144,236,.1); }
.avatar-preview { width:72px; height:72px; border-radius:50%; object-fit:cover; display:none; margin:0 auto 8px; border:3px solid var(--border); }
.drop-zone.small .avatar-preview { width:64px; height:64px; }
.chat-screen { height:100%; }
.chat-container { display:flex; height:100%; }
.sidebar { width:340px; background: var(--sidebar); border-right:1px solid var(--border); display:flex; flex-direction:column; }
.sidebar-header { height:58px; display:flex; align-items:center; justify-content:space-between; padding:0 18px; background: var(--header); border-bottom:1px solid var(--border); }
.header-title { font-weight:700; font-size:17px; }
.icon-btn { background:none; border:none; color: var(--text); font-size:21px; cursor:pointer; padding:6px 10px; border-radius:50%; transition:.2s; }
.icon-btn:hover { background: rgba(127,140,141,.15); }
.search-box { padding:10px 14px; border-bottom:1px solid var(--border); position:relative; }
.search-box input { width:100%; padding:9px 14px 9px 34px; background: var(--bg); border:1px solid var(--border); border-radius:22px; color: var(--text); outline:none; }
.search-box::before { content:'🔍'; position:absolute; left:24px; top:50%; transform:translateY(-50%); font-size:12px; opacity:.5; }
.search-box input:focus { border-color: var(--accent); }
.user-list { flex:1; overflow-y:auto; }
.user-item { display:flex; align-items:center; padding:11px 16px; cursor:pointer; transition:.15s; border-bottom:1px solid rgba(127,140,141,.06); }
.user-item:hover { background: rgba(127,140,141,.08); }
.user-item.active { background: rgba(51,144,236,.18); }
.user-item .last-msg { font-size:12px; color: var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; margin-top:2px; }
.avatar { border-radius:50%; display:flex; align-items:center; justify-content:center; font-weight:700; color:#fff; position:relative; flex-shrink:0; overflow:hidden; background-size: cover; background-position: center; }
.avatar-48 { width:48px; height:48px; font-size:20px; }
.avatar-40 { width:40px; height:40px; font-size:16px; }
.avatar-96 { width:96px; height:96px; font-size:40px; }
.avatar span { z-index:1; }
.avatar img { width:100%; height:100%; object-fit:cover; position:absolute; inset:0; z-index:2; }
.group-avatar { border-radius:12px; }
.online-dot { position:absolute; bottom:2px; right:2px; width:14px; height:14px; background:#4cd137; border:2px solid var(--sidebar); border-radius:50%; z-index:3; box-shadow:0 0 0 1px rgba(0,0,0,.2); }
.user-info { margin-left:13px; overflow:hidden; flex:1; }
.user-name { font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.user-status { font-size:13px; color: var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.chat-main { flex:1; display:flex; flex-direction:column; background: var(--bg); }
.chat-header { height:58px; display:flex; align-items:center; padding:0 16px; background: var(--header); border-bottom:1px solid var(--border); }
.chat-header-info { flex:1; margin-left:13px; min-width:0; }
.chat-title { font-weight:700; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.chat-subtitle { font-size:13px; color: var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.back-btn { background:none; border:none; color: var(--text); font-size:24px; cursor:pointer; margin-right:6px; padding:4px 10px; border-radius:50%; }
.back-btn:hover { background: rgba(127,140,141,.12); }
.typing-indicator { min-height:24px; padding:4px 18px; font-size:13px; color: var(--accent); font-style:italic; }
.pinned-message { background: rgba(51,144,236,.12); border-bottom:1px solid var(--border); padding:8px 16px; display:flex; align-items:center; gap:10px; cursor:pointer; }
.pinned-label { font-size:11px; color: var(--accent); font-weight:700; text-transform:uppercase; }
.pinned-text { flex:1; font-size:13px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; color: var(--text); }
.messages-area { flex:1; overflow-y:auto; padding:16px; display:flex; flex-direction:column; }
.message-bubble { max-width:min(80%, 580px); padding:7px 11px; margin:3px 0; border-radius:16px; position:relative; animation: msgIn .25s cubic-bezier(.25,.46,.45,.94); word-break:break-word; line-height:1.35; box-shadow:0 1px 2px var(--shadow); user-select:text; }
.message-own { align-self:flex-end; background: var(--bubble-own); border-bottom-right-radius:3px; }
.message-other { align-self:flex-start; background: var(--bubble-other); border-bottom-left-radius:3px; }
.message-system { align-self:center; color: var(--muted); font-size:13px; margin:10px 0; padding:4px 12px; background: rgba(127,140,141,.12); border-radius:12px; animation: msgIn .25s ease-out; }
.message-text { white-space:pre-wrap; }
.message-meta { display:flex; align-items:center; justify-content:flex-end; gap:5px; font-size:11px; margin-top:4px; color: rgba(255,255,255,.6); }
.message-own .message-meta { color: rgba(255,255,255,.75); }
.ticks { font-family: sans-serif; letter-spacing:-2px; }
.ticks-sent { color: rgba(255,255,255,.5); }
.ticks-delivered { color: rgba(255,255,255,.5); }
.ticks-read { color: #63b8ff; }
.reply-preview { background: rgba(0,0,0,.15); border-left:2px solid var(--accent); padding:5px 8px; border-radius:8px; margin-bottom:5px; font-size:13px; cursor:pointer; }
.reply-preview .reply-name { color: var(--accent); font-weight:600; }
.reply-preview .reply-text { color: var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.edited-label { font-size:10px; opacity:.7; margin-left:4px; }
.reactions { display:flex; flex-wrap:wrap; gap:4px; margin-top:4px; }
.reaction { background: rgba(0,0,0,.25); border-radius:10px; padding:2px 6px; font-size:13px; cursor:pointer; user-select:none; transition:.15s; }
.reaction.active { background: rgba(51,144,236,.35); }
.reaction-add { font-size:13px; cursor:pointer; opacity:.6; }
.reaction-add:hover { opacity:1; }
.input-area { display:flex; align-items:center; padding:10px 14px; background: var(--header); gap:8px; }
.reply-bar { width:100%; background: rgba(0,0,0,.15); padding:6px 14px; font-size:13px; display:flex; align-items:center; justify-content:space-between; }
.reply-bar span { color: var(--muted); }
.attach-btn { background:none; border:none; color: var(--muted); font-size:22px; cursor:pointer; padding:6px; border-radius:50%; transition:.2s; }
.attach-btn:hover { background: rgba(127,140,141,.12); color: var(--text); }
.input-area input[type=text] { flex:1; padding:11px 16px; background: var(--bg); border:1px solid var(--border); border-radius:22px; color: var(--text); outline:none; }
.input-area input:focus { border-color: var(--accent); }
.send-btn { width:42px; height:42px; border-radius:50%; background: var(--accent); border:none; color:#fff; cursor:pointer; display:flex; align-items:center; justify-content:center; transition:.2s; flex-shrink:0; }
.send-btn:hover { filter: brightness(1.1); }
.record-btn { width:42px; height:42px; border-radius:50%; background: transparent; border:1px solid var(--border); color: var(--muted); cursor:pointer; display:flex; align-items:center; justify-content:center; transition:.2s; flex-shrink:0; }
.record-btn.recording { background: #e74c3c; color:#fff; border-color:#e74c3c; animation: pulse 1s infinite; }
@keyframes pulse { 0%,100% { transform:scale(1);} 50%{transform:scale(1.05);} }
@keyframes msgIn { from { opacity:0; transform: translateY(12px) scale(.98);} to { opacity:1; transform: translateY(0) scale(1);} }
.file-attachment { display:flex; align-items:center; gap:10px; background: rgba(0,0,0,.2); border-radius:12px; padding:10px; margin-bottom:6px; min-width:200px; }
.file-icon { width:42px; height:42px; border-radius:10px; background: var(--accent); display:flex; align-items:center; justify-content:center; font-size:18px; flex-shrink:0; }
.file-info { flex:1; min-width:0; }
.file-name { font-weight:600; font-size:14px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.file-size { font-size:12px; color: var(--muted); }
.file-download { background:none; border:none; color: var(--text); cursor:pointer; font-size:18px; padding:4px; }
.image-attachment { max-width:260px; max-height:260px; border-radius:12px; cursor:pointer; object-fit:cover; display:block; margin-bottom:6px; }
.video-note { width:200px; height:200px; border-radius:50%; object-fit:cover; background:#000; cursor:pointer; display:block; margin-bottom:6px; border:3px solid var(--border); }
.voice-message { display:flex; align-items:center; gap:10px; background: rgba(0,0,0,.2); border-radius:20px; padding:8px 12px; min-width:220px; margin-bottom:6px; }
.voice-play { width:34px; height:34px; border-radius:50%; background: var(--accent); border:none; color:#fff; cursor:pointer; display:flex; align-items:center; justify-content:center; flex-shrink:0; }
.voice-wave { display:flex; align-items:center; gap:2px; height:28px; flex:1; }
.voice-bar { width:3px; background: rgba(255,255,255,.5); border-radius:2px; transition:.1s; }
.voice-bar.active { background: var(--accent); }
.voice-time { font-size:12px; color: var(--muted); min-width:36px; text-align:right; }
.modal { position:fixed; inset:0; background:rgba(0,0,0,.65); display:flex; align-items:center; justify-content:center; z-index:100; padding:16px; backdrop-filter: blur(2px); }
.modal-content { width:100%; max-width:400px; background: var(--sidebar); border-radius:16px; overflow:hidden; box-shadow:0 20px 60px rgba(0,0,0,.5); }
.modal-header { display:flex; justify-content:space-between; align-items:center; padding:14px 18px; border-bottom:1px solid var(--border); font-weight:700; }
.modal-header button { background:none; border:none; color: var(--text); font-size:26px; cursor:pointer; }
.modal-body { padding:18px; }
.profile-name { text-align:center; margin:10px 0; font-weight:700; font-size:19px; }
.theme-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:10px; margin:12px 0; }
.theme-option { aspect-ratio:1; border-radius:12px; border:2px solid transparent; cursor:pointer; display:flex; align-items:center; justify-content:center; font-size:11px; font-weight:600; text-align:center; padding:4px; transition:.2s; color:#fff; text-shadow:0 1px 2px rgba(0,0,0,.6); }
.theme-option:hover { transform:scale(1.05); }
.theme-option.active { border-color: var(--accent); box-shadow:0 0 0 2px var(--accent); }
.context-menu { position:fixed; background: var(--sidebar); border:1px solid var(--border); border-radius:10px; overflow:hidden; z-index:200; min-width:180px; box-shadow:0 8px 24px rgba(0,0,0,.4); }
.context-menu button { width:100%; padding:11px 16px; background:none; border:none; color: var(--text); cursor:pointer; text-align:left; font-size:14px; display:flex; align-items:center; gap:8px; }
.context-menu button:hover { background: rgba(127,140,141,.12); }
.reaction-picker { position:fixed; background: var(--sidebar); border:1px solid var(--border); border-radius:20px; padding:8px 12px; z-index:250; display:flex; gap:8px; box-shadow:0 8px 24px rgba(0,0,0,.4); }
.reaction-picker span { font-size:22px; cursor:pointer; transition:.15s; }
.reaction-picker span:hover { transform:scale(1.3); }
.toast { position:fixed; bottom:24px; left:50%; transform:translateX(-50%); background: rgba(30,30,30,.95); color:#fff; padding:12px 22px; border-radius:24px; z-index:300; font-size:14px; box-shadow:0 8px 24px rgba(0,0,0,.35); }
.media-preview { position:fixed; inset:0; background:rgba(0,0,0,.9); display:flex; align-items:center; justify-content:center; z-index:400; padding:20px; }
.media-preview img, .media-preview video { max-width:90%; max-height:90%; border-radius:12px; }
.media-preview button { position:absolute; top:20px; right:20px; background:rgba(0,0,0,.5); border:none; color:#fff; font-size:24px; width:40px; height:40px; border-radius:50%; cursor:pointer; }
.record-panel { display:flex; align-items:center; gap:10px; flex:1; background: var(--bg); border:1px solid var(--border); border-radius:22px; padding:8px 14px; color: var(--accent); font-weight:600; }
.record-timer { font-variant-numeric: tabular-nums; }
.cancel-record { color: var(--muted); cursor:pointer; font-size:13px; }
.drag-overlay { position:fixed; inset:0; background:rgba(51,144,236,.15); border:4px dashed var(--accent); z-index:500; display:flex; align-items:center; justify-content:center; font-size:22px; font-weight:700; color: var(--accent); pointer-events:none; }
.group-form { display:flex; gap:8px; margin-bottom:12px; }
.group-form input { flex:1; padding:10px; background: var(--bg); border:1px solid var(--border); border-radius:8px; color: var(--text); outline:none; }
.member-select { max-height:180px; overflow-y:auto; border:1px solid var(--border); border-radius:8px; padding:6px; }
.member-option { display:flex; align-items:center; gap:8px; padding:6px; cursor:pointer; border-radius:6px; }
.member-option:hover { background: rgba(127,140,141,.1); }
.member-option input { width:auto; }
@media (max-width:768px) {
  #chatContainer.mobile .sidebar { width:100%; position:absolute; inset:0; z-index:10; }
  #chatContainer.mobile .chat-main { width:100%; position:absolute; inset:0; z-index:20; display:none; }
  #chatContainer.mobile.mobile-open .chat-main { display:flex; }
  #chatContainer.mobile.mobile-open .sidebar { display:none; }
  .message-bubble { max-width:85%; }
  .video-note { width:160px; height:160px; }
  .image-attachment { max-width:220px; }
}
</style>
</head>
<body>
<div id="app">
  <div id="authScreen" class="auth-screen">
    <div class="auth-card">
      <div class="auth-title">Telegram Clone Pro</div>
      <div class="auth-tabs">
        <button id="tabLogin" class="active">Login</button>
        <button id="tabRegister">Register</button>
      </div>
      <form id="loginForm" class="auth-form">
        <input id="loginUsername" placeholder="Username" required autocomplete="username">
        <input id="loginPassword" type="password" placeholder="Password" required autocomplete="current-password">
        <button type="submit" class="btn-primary">Sign In</button>
      </form>
      <form id="registerForm" class="auth-form hidden">
        <input id="regUsername" placeholder="Username" required autocomplete="off">
        <input id="regPassword" type="password" placeholder="Password (min 4)" required autocomplete="new-password">
        <input id="regAbout" placeholder="About (optional)" maxlength="140">
        <div id="regDrop" class="drop-zone">
          <input type="file" id="regAvatar" accept="image/*" class="hidden">
          <img id="regAvatarPreview" class="avatar-preview" alt="">
          <span id="regDropText">Click or drag avatar here</span>
        </div>
        <button type="submit" class="btn-primary">Create Account</button>
      </form>
    </div>
  </div>
  <div id="chatScreen" class="chat-screen hidden">
    <div id="chatContainer" class="chat-container">
      <aside class="sidebar">
        <div class="sidebar-header">
          <div class="header-title">Telegram</div>
          <div style="display:flex;gap:6px">
            <button id="newGroupBtn" class="icon-btn" title="New group">👥</button>
            <button id="themeBtn" class="icon-btn" title="Theme">🎨</button>
            <button id="profileBtn" class="icon-btn" title="Profile">☰</button>
          </div>
        </div>
        <div class="search-box">
          <input id="searchUsers" placeholder="Search..." autocomplete="off">
        </div>
        <div class="tabs-bar">
          <button id="tabChats" class="tab-btn active">Chats</button>
          <button id="tabUsers" class="tab-btn">Users</button>
        </div>
        <div id="chatsList" class="user-list"></div>
        <div id="usersList" class="user-list hidden"></div>
      </aside>
      <main class="chat-main">
        <div class="chat-header">
          <button id="backBtn" class="back-btn hidden">←</button>
          <div class="chat-header-info">
            <div id="chatTitle" class="chat-title">Global Chat</div>
            <div id="chatSubtitle" class="chat-subtitle"></div>
          </div>
          <button id="clearHistoryBtn" class="icon-btn hidden" title="Clear history">🗑</button>
        </div>
        <div id="pinnedMessage" class="pinned-message hidden">
          <span class="pinned-label">Pinned</span>
          <span id="pinnedText" class="pinned-text"></span>
          <button id="unpinBtn" class="icon-btn" style="font-size:14px;padding:2px">✕</button>
        </div>
        <div id="typingIndicator" class="typing-indicator"></div>
        <div id="messagesArea" class="messages-area"></div>
        <div id="replyBar" class="reply-bar hidden"><span id="replyBarText"></span><button id="cancelReply" class="icon-btn" style="font-size:14px;padding:2px">✕</button></div>
        <div id="inputArea" class="input-area">
          <button id="attachBtn" class="attach-btn" title="Attach file">📎</button>
          <button id="videoNoteBtn" class="attach-btn" title="Video circle">⏺</button>
          <input id="messageInput" type="text" placeholder="Write a message..." maxlength="1000" autocomplete="off">
          <button id="recordBtn" class="record-btn" title="Voice message">🎤</button>
          <button id="sendBtn" class="send-btn">➤</button>
        </div>
      </main>
    </div>
  </div>
</div>
<input type="file" id="fileInput" class="hidden">
<div id="profileModal" class="modal hidden">
  <div class="modal-content">
    <div class="modal-header"><span>Edit Profile</span><button class="modal-close" data-modal="profileModal">×</button></div>
    <div class="modal-body">
      <div id="profileDrop" class="drop-zone small">
        <input type="file" id="profileAvatar" accept="image/*" class="hidden">
        <img id="profileAvatarPreview" class="avatar-preview" alt="">
        <span id="profileDropText">Change avatar</span>
      </div>
      <div id="profileUsername" class="profile-name"></div>
      <input id="profileAbout" placeholder="About" maxlength="140">
      <button id="saveProfile" class="btn-primary">Save</button>
      <button id="logoutBtn" class="btn-secondary">Logout</button>
    </div>
  </div>
</div>
<div id="themeModal" class="modal hidden">
  <div class="modal-content">
    <div class="modal-header"><span>Choose Theme</span><button class="modal-close" data-modal="themeModal">×</button></div>
    <div class="modal-body"><div id="themeGrid" class="theme-grid"></div></div>
  </div>
</div>
<div id="groupModal" class="modal hidden">
  <div class="modal-content">
    <div class="modal-header"><span>New Group</span><button class="modal-close" data-modal="groupModal">×</button></div>
    <div class="modal-body">
      <div class="group-form"><input id="groupName" placeholder="Group name" maxlength="40"></div>
      <div id="groupMembers" class="member-select"></div>
      <button id="createGroup" class="btn-primary">Create Group</button>
    </div>
  </div>
</div>
<div id="contextMenu" class="context-menu hidden"></div>
<div id="reactionPicker" class="reaction-picker hidden"><span>👍</span><span>❤️</span><span>😂</span><span>😮</span><span>😢</span><span>🎉</span><span>🔥</span><span>👏</span></div>
<div id="mediaPreview" class="media-preview hidden"><button>×</button></div>
<div id="dragOverlay" class="drag-overlay hidden">Drop files here</div>
<div id="toast" class="toast hidden"></div>
<script id="themes-data" type="application/json">${JSON.stringify(THEMES).replace(/</g, '\\u003c')}</script>
<script>
const THEMES_SERVER = JSON.parse(document.getElementById('themes-data').textContent);
const socket = io();
const App = { token: localStorage.getItem('token'), user: null, users: [], chats: [], currentChatId: 'global', typing: {}, selectedAvatar: null, replyTo: null, editId: null, contextMsg: null, theme: localStorage.getItem('theme') || 'dark', mediaRecorder: null, recordedChunks: [], recordingStart: 0 };
const q = (id) => document.getElementById(id);

function init() {
  applyTheme(App.theme);
  bindAuthTabs(); bindForms(); bindChatEvents(); bindProfile(); bindTheme(); bindGroup(); bindContextMenu(); bindDragDrop(); bindReactionPicker();
  checkMobile(); window.addEventListener('resize', checkMobile);
  socket.on('connect', () => { if (App.token) socket.emit('authenticate', {token: App.token}); });
  socket.on('logged_in', onLoggedIn);
  socket.on('auth_error', () => { logout(); showToast('Session expired'); });
  socket.on('register_error', (m) => showToast(m));
  socket.on('login_error', (m) => showToast(m));
  socket.on('profile_error', (m) => showToast(m));
  socket.on('error_message', (m) => showToast(m));
  socket.on('users_list', (list) => { App.users = list; renderChatList(); updateChatSubtitle(); });
  socket.on('chats_list', (list) => { App.chats = list; renderChatList(); });
  socket.on('unread_counts', (counts) => { App.chats.forEach((c) => { c.unread = counts[c.id] || 0; }); renderChatList(); });
  socket.on('message', (data) => handleIncoming(data.chatId, data.message));
  socket.on('system_message', (data) => handleIncoming(data.chatId, data));
  socket.on('history', (data) => { if (data.chatId === App.currentChatId) renderHistory(data.messages, data.pinnedId); });
  socket.on('chat_update', (data) => handleChatUpdate(data));
  socket.on('logged_out', () => showAuth());
  if (App.token) showChat(); else showAuth();
}

function applyTheme(themeName, persist) {
  const t = THEMES_SERVER[themeName] || THEMES_SERVER.dark;
  const root = document.documentElement;
  root.style.setProperty('--bg', t.bg); root.style.setProperty('--sidebar', t.sidebar);
  root.style.setProperty('--bubble-own', t.bubbleOwn); root.style.setProperty('--bubble-other', t.bubbleOther);
  root.style.setProperty('--accent', t.accent); root.style.setProperty('--text', t.text);
  root.style.setProperty('--muted', t.muted); root.style.setProperty('--border', t.border);
  root.style.setProperty('--header', t.header);
  App.theme = themeName; if (persist) { localStorage.setItem('theme', themeName); if (App.user) socket.emit('update_profile', {theme: themeName}); }
}
function bindAuthTabs() { q('tabLogin').addEventListener('click', () => switchTab('login')); q('tabRegister').addEventListener('click', () => switchTab('register')); }
function switchTab(tab) { if (tab === 'login') { q('loginForm').classList.remove('hidden'); q('registerForm').classList.add('hidden'); q('tabLogin').classList.add('active'); q('tabRegister').classList.remove('active'); } else { q('loginForm').classList.add('hidden'); q('registerForm').classList.remove('hidden'); q('tabLogin').classList.remove('active'); q('tabRegister').classList.add('active'); } }
function bindForms() {
  setupDropZone('regDrop', 'regAvatar', 'regAvatarPreview', 'regDropText', (b64) => { App.selectedAvatar = b64; }, true);
  q('registerForm').addEventListener('submit', (e) => { e.preventDefault(); const username = q('regUsername').value.trim().toLowerCase(); const password = q('regPassword').value; if (password.length < 4) return showToast('Password min 4 chars'); socket.emit('register', {username, password, avatarBase64: App.selectedAvatar, about: q('regAbout').value, theme: App.theme}); });
  q('loginForm').addEventListener('submit', (e) => { e.preventDefault(); socket.emit('login', {username: q('loginUsername').value.trim().toLowerCase(), password: q('loginPassword').value}); });
}
function setupDropZone(zoneId, inputId, previewId, textId, callback, compress) {
  const zone = q(zoneId), input = q(inputId), preview = q(previewId), text = q(textId);
  zone.addEventListener('click', () => input.click());
  zone.addEventListener('dragover', (e) => { e.preventDefault(); zone.classList.add('dragover'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
  zone.addEventListener('drop', (e) => { e.preventDefault(); zone.classList.remove('dragover'); handleFiles(e.dataTransfer.files); });
  input.addEventListener('change', () => handleFiles(input.files));
  function handleFiles(files) { if (!files || !files[0]) return; if (!files[0].type.startsWith('image/')) return showToast('Please select an image'); if (compress) compressImage(files[0], (dataUrl) => { preview.src = dataUrl; preview.style.display = 'block'; if (text) text.style.display = 'none'; callback(dataUrl); }); else { const r = new FileReader(); r.onload = (e) => { preview.src = e.target.result; preview.style.display = 'block'; if (text) text.style.display = 'none'; callback(e.target.result); }; r.readAsDataURL(files[0]); } }
}
function compressImage(file, callback, maxLen) {
  maxLen = maxLen || 64000;
  const reader = new FileReader();
  reader.onload = (e) => {
    const img = new Image();
    img.onload = () => { let size = 256, quality = 0.9; function tryCompress() { const canvas = document.createElement('canvas'); canvas.width = size; canvas.height = size; const ctx = canvas.getContext('2d'); const scale = Math.max(size / img.width, size / img.height); const w = img.width * scale, h = img.height * scale; ctx.fillStyle = '#17212b'; ctx.fillRect(0, 0, size, size); ctx.drawImage(img, (size - w) / 2, (size - h) / 2, w, h); const dataUrl = canvas.toDataURL('image/jpeg', quality); if (dataUrl.length > maxLen && quality > 0.3) { quality -= 0.1; tryCompress(); } else if (dataUrl.length > maxLen && size > 96) { size = Math.floor(size * 0.75); quality = 0.9; tryCompress(); } else callback(dataUrl); } tryCompress(); };
    img.src = e.target.result;
  };
  reader.readAsDataURL(file);
}
function onLoggedIn(data) { App.token = data.token; App.user = data.user; localStorage.setItem('token', data.token); applyTheme(data.user.theme || App.theme, false); updateProfileUI(); showChat(); App.currentChatId = 'global'; socket.emit('get_users'); socket.emit('get_chats'); renderHeader(); }
function showChat() { q('authScreen').classList.add('hidden'); q('chatScreen').classList.remove('hidden'); }
function showAuth() { q('authScreen').classList.remove('hidden'); q('chatScreen').classList.add('hidden'); }

function renderHeader() {
  const chat = App.chats.find((c) => c.id === App.currentChatId);
  q('chatTitle').textContent = chat ? chat.name : 'Global Chat';
  updateChatSubtitle();
  const canClear = App.currentChatId === 'global' && App.user && App.user.isAdmin;
  q('clearHistoryBtn').classList.toggle('hidden', !canClear);
}
function updateChatSubtitle() {
  if (App.currentChatId === 'global') { q('chatSubtitle').textContent = App.users.length + ' users'; return; }
  const chat = App.chats.find((c) => c.id === App.currentChatId);
  if (!chat) return;
  if (chat.type === 'private') {
    const u = App.users.find((x) => x.id === chat.userId);
    q('chatSubtitle').textContent = u && u.online ? 'online' : (u ? formatLastSeen(u.lastSeen) : 'last seen recently');
  } else if (chat.type === 'group') { q('chatSubtitle').textContent = chat.membersCount + ' members'; }
}
function formatLastSeen(ts) {
  const diff = Math.floor((Date.now() - ts) / 1000);
  if (diff < 60) return 'last seen just now';
  if (diff < 3600) return 'last seen ' + Math.floor(diff / 60) + ' minutes ago';
  if (diff < 86400) return 'last seen ' + Math.floor(diff / 3600) + ' hours ago';
  return 'last seen ' + Math.floor(diff / 86400) + ' days ago';
}

function bindChatEvents() {
  q('sendBtn').addEventListener('click', sendMessage);
  q('messageInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); sendMessage(); } });
  q('messageInput').addEventListener('input', onTyping);
  q('searchUsers').addEventListener('input', renderChatList);
  q('backBtn').addEventListener('click', () => { q('chatContainer').classList.remove('mobile-open'); q('backBtn').classList.add('hidden'); });
  q('clearHistoryBtn').addEventListener('click', () => { if (confirm('Clear global history?')) socket.emit('clear_history'); });
  q('attachBtn').addEventListener('click', () => q('fileInput').click());
  q('fileInput').addEventListener('change', () => handleFileUpload(q('fileInput').files[0]));
  q('videoNoteBtn').addEventListener('click', toggleVideoRecording);
  q('recordBtn').addEventListener('click', toggleAudioRecording);
  q('cancelReply').addEventListener('click', () => { App.replyTo = null; App.editId = null; q('replyBar').classList.add('hidden'); q('messageInput').placeholder = 'Write a message...'; });
  q('unpinBtn').addEventListener('click', () => socket.emit('pin_message', {chatId: App.currentChatId, messageId: null}));
  q('pinnedMessage').addEventListener('click', () => { const id = q('pinnedMessage').dataset.id; if (id) { const el = q('messagesArea').querySelector('[data-id="' + id + '"]'); if (el) el.scrollIntoView({behavior:'smooth', block:'center'}); } });
}

function openChat(chatId) {
  App.currentChatId = chatId; App.replyTo = null; App.editId = null; q('replyBar').classList.add('hidden');
  q('messagesArea').innerHTML = ''; App.typing = {}; q('typingIndicator').textContent = '';
  renderHeader(); renderChatList();
  socket.emit('get_history', {chatId});
  if (window.innerWidth <= 768) { q('chatContainer').classList.add('mobile-open'); q('backBtn').classList.remove('hidden'); }
}

function renderChatList() {
  const term = q('searchUsers').value.trim().toLowerCase();
  const list = q('chatsList'); list.innerHTML = '';
  const showChats = !q('chatsList').classList.contains('hidden');
  if (showChats) {
    // Render chats tab
    App.chats.forEach((chat) => {
      if (term) { const s = (chat.name + ' ' + (chat.lastMessage && chat.lastMessage.text || '')).toLowerCase(); if (s.indexOf(term) === -1) return; }
      const item = document.createElement('div');
      item.className = 'user-item' + (chat.id === App.currentChatId ? ' active' : '');
      item.appendChild(getChatAvatarHTML(chat));
      const info = document.createElement('div'); info.className = 'user-info';
      const top = document.createElement('div'); top.style.display = 'flex'; top.style.justifyContent = 'space-between';
      const name = document.createElement('div'); name.className = 'user-name'; name.textContent = chat.name;
      const time = document.createElement('div'); time.style.fontSize = '11px'; time.style.color = 'var(--muted)'; time.textContent = chat.lastMessage ? formatTime(chat.lastMessage.timestamp) : '';
      top.appendChild(name); top.appendChild(time);
      const last = document.createElement('div'); last.className = 'last-msg';
      last.textContent = chat.lastMessage ? (chat.lastMessage.type === 'system' ? chat.lastMessage.text : (chat.lastMessage.sender && chat.lastMessage.sender.username ? chat.lastMessage.sender.username + ': ' : '') + previewText(chat.lastMessage)) : 'No messages';
      info.appendChild(top); info.appendChild(last);
      item.appendChild(info);
      // unread badge
      if (chat.unread > 0) {
        const badge = document.createElement('div');
        badge.style.cssText = 'background:var(--accent);color:#fff;border-radius:50%;font-size:11px;min-width:18px;height:18px;padding:2px 6px;margin-left:8px;display:flex;align-items:center;justify-content:center;';
        badge.textContent = chat.unread > 99 ? '99+' : chat.unread;
        item.appendChild(badge);
      }
      item.addEventListener('click', () => openChat(chat.id));
      list.appendChild(item);
    });
  } else {
    // Render users tab
    App.users.forEach((u) => {
      if (u.id === App.user.id) return;
      if (term && u.username.toLowerCase().indexOf(term) === -1 && (!u.about || u.about.toLowerCase().indexOf(term) === -1)) return;
      const item = document.createElement('div');
      item.className = 'user-item';
      item.appendChild(getUserAvatarHTML(u));
      const info = document.createElement('div'); info.className = 'user-info';
      const name = document.createElement('div'); name.className = 'user-name'; name.textContent = u.username;
      const status = document.createElement('div'); status.className = 'user-status'; status.textContent = u.online ? 'online' : formatLastSeen(u.lastSeen);
      info.appendChild(name); info.appendChild(status);
      item.appendChild(info);
      item.addEventListener('click', () => startPrivateChat(u.id));
      list.appendChild(item);
    });
  }
}

function getUserAvatarHTML(user) {
  const div = document.createElement('div'); div.className = 'avatar avatar-48';
  div.style.backgroundColor = stringToColor(user.username);
  const initial = document.createElement('span'); initial.textContent = getInitials(user.username); div.appendChild(initial);
  if (user.avatar) { const img = document.createElement('img'); img.src = user.avatar; img.alt = ''; div.appendChild(img); }
  if (user.online) { const dot = document.createElement('span'); dot.className = 'online-dot'; div.appendChild(dot); }
  return div;
}

function startPrivateChat(otherUserId) {
  const userId = App.user.id;
  const key = getPrivateKey(userId, otherUserId);
  // Check if chat exists in memory (server will create if not)
  socket.emit('open_private_chat', { otherUserId }, (chat) => {
    if (chat) {
      // Add to local chats list if not exists
      if (!App.chats.find((c) => c.id === chat.id)) {
        App.chats.push(chat);
      }
      openChat(chat.id);
      switchTab('chats');
    } else {
      showToast('Failed to open chat');
    }
  });
}

function switchTab(tab) {
  if (tab === 'chats') {
    q('chatsList').classList.remove('hidden');
    q('usersList').classList.add('hidden');
    q('tabChats').classList.add('active');
    q('tabUsers').classList.remove('active');
  } else {
    q('chatsList').classList.add('hidden');
    q('usersList').classList.remove('hidden');
    q('tabChats').classList.remove('active');
    q('tabUsers').classList.add('active');
  }
  renderChatList();
}
function previewText(msg) { if (msg.deletedForAll) return 'Message deleted'; if (msg.mediaType === 'image') return '📷 Photo'; if (msg.mediaType === 'video_note') return '🎥 Video circle'; if (msg.mediaType === 'voice') return '🎤 Voice'; if (msg.mediaType === 'file') return '📎 ' + (msg.fileName || 'File'); return msg.text || ''; }
function getChatAvatarHTML(chat) {
  const div = document.createElement('div'); div.className = 'avatar avatar-48' + (chat.type === 'group' ? ' group-avatar' : '');
  if (chat.type === 'private') {
    const u = App.users.find((x) => x.id === chat.userId);
    div.style.backgroundColor = stringToColor(u ? u.username : 'x');
    const initial = document.createElement('span'); initial.textContent = u ? getInitials(u.username) : '?'; div.appendChild(initial);
    if (u && u.avatar) { const img = document.createElement('img'); img.src = u.avatar; img.alt = ''; div.appendChild(img); }
    if (u && u.online) { const dot = document.createElement('span'); dot.className = 'online-dot'; div.appendChild(dot); }
  } else {
    div.style.backgroundColor = '#2b5278';
    const initial = document.createElement('span'); initial.textContent = chat.name ? chat.name[0].toUpperCase() : 'G'; div.appendChild(initial);
  }
  return div;
}
function getInitials(name) { return (name && name[0] ? name[0].toUpperCase() : '?'); }
function stringToColor(str) { let hash = 0; for (let i = 0; i < str.length; i++) hash = str.charCodeAt(i) + ((hash << 5) - hash); const h = Math.abs(hash) % 360; return 'hsl(' + h + ', 55%, 45%)'; }

function handleIncoming(chatId, msg) {
  const chat = App.chats.find((c) => c.id === chatId);
  if (chat) { chat.lastMessage = msg; if (chatId !== App.currentChatId) chat.unread = (chat.unread || 0) + 1; }
  if (chatId !== App.currentChatId) { renderChatList(); playNotification(); return; }
  if (!(msg.sender && msg.sender.id === (App.user && App.user.id)) && msg.type !== 'system') playNotification();
  appendMessage(msg);
  renderChatList();
}
function appendMessage(msg) { q('messagesArea').appendChild(createMessageEl(msg)); scrollToBottom(); }

function createMessageEl(msg) {
  const div = document.createElement('div');
  if (msg.type === 'system') { div.className = 'message-system'; div.textContent = msg.text; return div; }
  const isOwn = msg.sender && msg.sender.id === (App.user && App.user.id);
  div.className = 'message-bubble ' + (isOwn ? 'message-own' : 'message-other');
  div.dataset.id = msg.id;
  if (msg.replyTo) div.appendChild(createReplyPreview(msg.replyTo));
  if (msg.mediaType === 'image' && msg.fileUrl) div.appendChild(createImageAttachment(msg));
  else if (msg.mediaType === 'video_note' && msg.fileUrl) div.appendChild(createVideoNoteAttachment(msg));
  else if (msg.mediaType === 'voice' && msg.fileUrl) div.appendChild(createVoiceAttachment(msg));
  else if (msg.mediaType === 'file' && msg.fileUrl) div.appendChild(createFileAttachment(msg));
  else { const text = document.createElement('div'); text.className = 'message-text'; text.textContent = msg.deletedForAll ? 'This message was deleted' : msg.text; div.appendChild(text); }
  if (!msg.deletedForAll) {
    const meta = document.createElement('div'); meta.className = 'message-meta';
    const time = document.createElement('span'); time.textContent = formatTime(msg.timestamp); meta.appendChild(time);
    if (msg.editedAt) { const ed = document.createElement('span'); ed.className = 'edited-label'; ed.textContent = 'edited'; meta.appendChild(ed); }
    if (isOwn && (App.currentChatId.startsWith('private:') || App.chats.find((c) => c.id === App.currentChatId && c.type === 'private'))) { const status = document.createElement('span'); status.className = 'ticks ' + getStatusClass(msg.status); status.textContent = getStatusTicks(msg.status); meta.appendChild(status); }
    div.appendChild(meta);
    div.appendChild(createReactions(msg));
  }
  div.addEventListener('contextmenu', (e) => showContextMenu(e, msg));
  // swipe right to reply
  let startX = 0;
  div.addEventListener('touchstart', (e) => { startX = e.touches[0].clientX; }, {passive: true});
  div.addEventListener('touchend', (e) => { const diff = e.changedTouches[0].clientX - startX; if (diff > 60 && !msg.deletedForAll) setReplyTo(msg); });
  return div;
}
function createReplyPreview(reply) {
  const div = document.createElement('div'); div.className = 'reply-preview';
  div.innerHTML = '<div class="reply-name">' + escapeHTML(reply.senderName || 'Unknown') + '</div><div class="reply-text">' + escapeHTML(previewText(reply)) + '</div>';
  div.addEventListener('click', () => { const el = q('messagesArea').querySelector('[data-id="' + reply.id + '"]'); if (el) el.scrollIntoView({behavior:'smooth', block:'center'}); });
  return div;
}
function createReactions(msg) {
  const wrap = document.createElement('div'); wrap.className = 'reactions';
  const counts = msg.reactions || {};
  Object.keys(counts).forEach((emoji) => {
    const r = document.createElement('span'); r.className = 'reaction' + (counts[emoji].includes(App.user && App.user.id) ? ' active' : ''); r.textContent = emoji + ' ' + counts[emoji].length;
    r.addEventListener('click', (e) => { e.stopPropagation(); socket.emit('add_reaction', {chatId: App.currentChatId, messageId: msg.id, emoji}); });
    wrap.appendChild(r);
  });
  if (!msg.deletedForAll) { const add = document.createElement('span'); add.className = 'reaction-add'; add.textContent = '➕'; add.addEventListener('click', (e) => showReactionPicker(e, msg.id)); wrap.appendChild(add); }
  return wrap;
}

function createImageAttachment(msg) { const img = document.createElement('img'); img.className = 'image-attachment'; img.src = msg.fileUrl; img.alt = ''; img.addEventListener('click', () => showMediaPreview(msg.fileUrl, 'image')); return img; }
function createVideoNoteAttachment(msg) { const wrap = document.createElement('div'); wrap.style.cssText = 'position:relative;width:200px;height:200px'; const video = document.createElement('video'); video.className = 'video-note'; video.src = msg.fileUrl; video.muted = true; video.loop = true; video.playsInline = true; const badge = document.createElement('div'); badge.textContent = '⏵'; badge.style.cssText = 'position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:32px;color:#fff;text-shadow:0 2px 6px rgba(0,0,0,.5);pointer-events:none;'; video.addEventListener('click', () => { video.muted = !video.muted; if (video.paused) { video.play(); badge.style.display = 'none'; } else { video.pause(); badge.style.display = 'flex'; } }); wrap.appendChild(video); wrap.appendChild(badge); return wrap; }
function createVoiceAttachment(msg) { const wrap = document.createElement('div'); wrap.className = 'voice-message'; const play = document.createElement('button'); play.className = 'voice-play'; play.innerHTML = '▶'; const wave = document.createElement('div'); wave.className = 'voice-wave'; const bars = msg.waveform ? msg.waveform.split(',').map((h) => { const b = document.createElement('div'); b.className = 'voice-bar'; b.style.height = Math.max(4, parseInt(h)) + 'px'; wave.appendChild(b); return b; }) : []; const time = document.createElement('div'); time.className = 'voice-time'; time.textContent = formatDuration(msg.duration || 0); wrap.appendChild(play); wrap.appendChild(wave); wrap.appendChild(time); const audio = new Audio(msg.fileUrl); audio.addEventListener('timeupdate', () => { const p = audio.duration ? audio.currentTime / audio.duration : 0; const idx = Math.floor(p * bars.length); bars.forEach((b, i) => b.classList.toggle('active', i <= idx)); time.textContent = formatDuration(audio.duration - audio.currentTime); }); audio.addEventListener('ended', () => { play.innerHTML = '▶'; bars.forEach((b) => b.classList.remove('active')); time.textContent = formatDuration(msg.duration || 0); }); play.addEventListener('click', () => { if (audio.paused) { audio.play(); play.innerHTML = '⏸'; } else { audio.pause(); play.innerHTML = '▶'; } }); return wrap; }
function createFileAttachment(msg) { const div = document.createElement('div'); div.className = 'file-attachment'; const icon = document.createElement('div'); icon.className = 'file-icon'; icon.textContent = '📄'; const info = document.createElement('div'); info.className = 'file-info'; const name = document.createElement('div'); name.className = 'file-name'; name.textContent = msg.fileName || 'file'; const size = document.createElement('div'); size.className = 'file-size'; size.textContent = formatBytes(msg.fileSize || 0); info.appendChild(name); info.appendChild(size); const dl = document.createElement('button'); dl.className = 'file-download'; dl.innerHTML = '⬇'; dl.addEventListener('click', () => downloadDataUrl(msg.fileUrl, msg.fileName || 'download')); div.appendChild(icon); div.appendChild(info); div.appendChild(dl); return div; }
function getStatusClass(s) { if (s === 'read') return 'ticks-read'; if (s === 'delivered') return 'ticks-delivered'; return 'ticks-sent'; }
function getStatusTicks(s) { return s === 'sent' ? '✓' : '✓✓'; }

function handleChatUpdate(data) {
  if (data.type === 'message_edited' || data.type === 'message_deleted' || data.type === 'reactions_updated') {
    const chat = App.chats.find((c) => c.id === data.chatId);
    if (chat && data.lastMessage) chat.lastMessage = data.lastMessage;
    renderChatList();
    if (data.chatId === App.currentChatId) {
      const bubble = q('messagesArea').querySelector('[data-id="' + data.messageId + '"]');
      if (bubble) {
        const area = q('messagesArea'); const idx = Array.from(area.children).indexOf(bubble);
        const newEl = createMessageEl(data.message || data);
        area.replaceChild(newEl, bubble);
      }
    }
  } else if (data.type === 'pinned_changed') {
    if (data.chatId === App.currentChatId) showPinned(data.pinnedId, data.pinnedText);
  } else if (data.type === 'chat_created') {
    socket.emit('get_chats');
  }
}

function renderHistory(messages, pinnedId) {
  const area = q('messagesArea'); area.innerHTML = '';
  messages.forEach((m) => area.appendChild(createMessageEl(m)));
  if (pinnedId) {
    const pinned = messages.find((m) => m.id === pinnedId);
    if (pinned) showPinned(pinnedId, previewText(pinned));
  } else { q('pinnedMessage').classList.add('hidden'); }
  scrollToBottom();
}
function showPinned(id, text) { q('pinnedMessage').dataset.id = id; q('pinnedText').textContent = text || ''; q('pinnedMessage').classList.remove('hidden'); }

function sendMessage() {
  const input = q('messageInput');
  let text = input.value.trim();
  if (App.editId) { socket.emit('edit_message', {chatId: App.currentChatId, messageId: App.editId, text}); input.value = ''; App.editId = null; q('replyBar').classList.add('hidden'); return; }
  if (!text) return; if (text.length > 1000) text = text.slice(0, 1000);
  const payload = { text, mediaType: 'text', replyTo: App.replyTo ? { id: App.replyTo.id, senderName: App.replyTo.sender ? App.replyTo.sender.username : 'Unknown', text: previewText(App.replyTo) } : null };
  socket.emit('send_message', Object.assign({}, payload, {chatId: App.currentChatId}));
  input.value = ''; App.replyTo = null; App.editId = null; q('replyBar').classList.add('hidden'); q('messageInput').placeholder = 'Write a message...';
  stopTyping();
}

function setReplyTo(msg) { App.replyTo = msg; App.editId = null; q('replyBarText').textContent = 'Reply to ' + (msg.sender ? msg.sender.username : 'Unknown'); q('replyBar').classList.remove('hidden'); q('messageInput').focus(); }
function setEditTo(msg) { App.editId = msg.id; App.replyTo = null; q('messageInput').value = msg.text || ''; q('replyBarText').textContent = 'Edit message'; q('replyBar').classList.remove('hidden'); q('messageInput').focus(); }

function handleFileUpload(file) {
  if (!file) return; if (file.size > 2.9 * 1024 * 1024) { showToast('File max 3MB'); return; }
  const reader = new FileReader();
  reader.onload = (e) => { const payload = { text: file.name, mediaType: file.type.startsWith('image/') ? 'image' : 'file', fileUrl: e.target.result, fileName: file.name, fileSize: file.size, mime: file.type }; sendMediaMessage(payload); };
  reader.readAsDataURL(file);
}
function sendMediaMessage(payload) { socket.emit('send_message', Object.assign({}, payload, {chatId: App.currentChatId, replyTo: App.replyTo ? { id: App.replyTo.id, senderName: App.replyTo.sender ? App.replyTo.sender.username : 'Unknown', text: previewText(App.replyTo) } : null})); App.replyTo = null; q('replyBar').classList.add('hidden'); }

async function toggleVideoRecording() { const btn = q('videoNoteBtn'); if (App.videoRecorder && App.videoRecorder.state === 'recording') { stopVideoRecording(); return; } if (App.mediaRecorder && App.mediaRecorder.state === 'recording') { showToast('Audio recording in progress'); return; } try { const stream = await navigator.mediaDevices.getUserMedia({video: {width: 480, height: 480}, audio: true}); App.videoStream = stream; App.videoRecorder = new MediaRecorder(stream, {mimeType: 'video/webm;codecs=vp8,opus'}); App.videoChunks = []; App.recordingStart = Date.now(); App.videoRecorder.ondataavailable = (e) => { if (e.data.size > 0) App.videoChunks.push(e.data); }; App.videoRecorder.onstop = () => { processVideoRecording(); stream.getTracks().forEach((t) => t.stop()); }; App.videoRecorder.start(100); btn.classList.add('recording'); showVideoRecordingPanel(true, stream); } catch (e) { showToast('Camera access denied or unsupported'); console.error(e); } }
function stopVideoRecording() { if (App.videoRecorder && App.videoRecorder.state === 'recording') App.videoRecorder.stop(); }
async function processVideoRecording() { showToast('Processing video circle...'); let blob = new Blob(App.videoChunks, {type: 'video/webm'}); const maxSize = 2.9 * 1024 * 1024; if (blob.size > maxSize) { showToast('Compressing video...'); const sizes = [192, 128]; for (const sz of sizes) { blob = await compressVideoBlob(blob, sz, 12); if (blob.size <= maxSize) break; } if (blob.size > maxSize) { showToast('Video too large even after compression (max 3MB)'); q('videoNoteBtn').classList.remove('recording'); showVideoRecordingPanel(false); return; } } const url = URL.createObjectURL(blob); const video = document.createElement('video'); video.src = url; video.muted = true; video.playsInline = true; video.onloadedmetadata = () => { const canvas = document.createElement('canvas'); canvas.width = 320; canvas.height = 320; const ctx = canvas.getContext('2d'); const size = Math.min(video.videoWidth, video.videoHeight); const sx = (video.videoWidth - size) / 2, sy = (video.videoHeight - size) / 2; video.currentTime = 0; video.onseeked = () => { ctx.drawImage(video, sx, sy, size, size, 0, 0, 320, 320); const reader = new FileReader(); reader.onload = (e) => { sendMediaMessage({text: 'Video circle', mediaType: 'video_note', fileUrl: e.target.result, fileName: 'circle.webm', fileSize: blob.size, mime: 'video/webm', duration: Math.floor(video.duration || (Date.now() - App.recordingStart)/1000)}); URL.revokeObjectURL(url); q('videoNoteBtn').classList.remove('recording'); showVideoRecordingPanel(false); }; reader.readAsDataURL(blob); }; }; }
async function compressVideoBlob(inputBlob, targetSize, fps) { return new Promise((resolve) => { try { const url = URL.createObjectURL(inputBlob); const video = document.createElement('video'); video.src = url; video.muted = true; video.playsInline = true; video.crossOrigin = 'anonymous'; video.onloadedmetadata = async () => { try { const canvas = document.createElement('canvas'); canvas.width = targetSize; canvas.height = targetSize; const ctx = canvas.getContext('2d'); const size = Math.min(video.videoWidth, video.videoHeight); const sx = (video.videoWidth - size) / 2, sy = (video.videoHeight - size) / 2; const stream = canvas.captureStream(fps); let recorder; try { recorder = new MediaRecorder(stream, {mimeType: 'video/webm;codecs=vp8'}); } catch (e) { recorder = new MediaRecorder(stream); } const chunks = []; recorder.ondataavailable = (ev) => { if (ev.data.size > 0) chunks.push(ev.data); }; const totalFrames = Math.min(300, Math.max(20, Math.floor((video.duration || 5) * fps))); let frame = 0, ok = false; const startRec = async () => { try { await video.play(); } catch (e) {} recorder.start(); const drawFrame = () => { if (!ok) return; if (frame >= totalFrames || video.ended) { ok = false; try { recorder.stop(); } catch (e) {} return; } ctx.drawImage(video, sx, sy, size, size, 0, 0, targetSize, targetSize); frame++; if (frame < totalFrames) { video.currentTime = Math.min(video.duration || 5, frame / fps); } else { try { recorder.stop(); } catch (e) {} } }; video.addEventListener('seeked', drawFrame); drawFrame(); }; recorder.onstop = () => { ok = false; URL.revokeObjectURL(url); const result = chunks.length ? new Blob(chunks, {type: 'video/webm'}) : inputBlob; resolve(result); }; ok = true; startRec(); setTimeout(() => { if (ok) { ok = false; try { recorder.stop(); } catch (e) {} } }, 15000); } catch (e) { console.error(e); URL.revokeObjectURL(url); resolve(inputBlob); } }; video.onerror = () => { URL.revokeObjectURL(url); resolve(inputBlob); }; } catch (e) { resolve(inputBlob); } }); }
function showVideoRecordingPanel(show, stream) { const input = q('messageInput'), attach = q('attachBtn'), micBtn = q('recordBtn'); if (show) { input.style.display = 'none'; attach.style.display = 'none'; micBtn.style.display = 'none'; const panel = document.createElement('div'); panel.id = 'videoRecordPanel'; panel.className = 'record-panel'; panel.style.justifyContent = 'space-between'; panel.innerHTML = '<div style="display:flex;align-items:center;gap:10px"><video id="liveVideo" autoplay muted playsinline style="width:36px;height:36px;border-radius:50%;object-fit:cover;background:#000;border:2px solid var(--accent);"></video><span class="record-timer" id="vRecordTimer" style="color:red">0:00</span></div><span class="cancel-record" id="cancelVRecord">Cancel</span>'; q('inputArea').insertBefore(panel, q('sendBtn')); q('liveVideo').srcObject = stream; q('cancelVRecord').addEventListener('click', () => { if (App.videoRecorder) { App.videoChunks = []; App.videoRecorder.stop(); } q('videoNoteBtn').classList.remove('recording'); showVideoRecordingPanel(false); }); App.vRecordInterval = setInterval(() => { const s = Math.floor((Date.now() - App.recordingStart) / 1000); q('vRecordTimer').textContent = Math.floor(s / 60) + ':' + (s % 60).toString().padStart(2, '0'); }, 1000); } else { input.style.display = ''; attach.style.display = ''; micBtn.style.display = ''; const panel = q('videoRecordPanel'); if (panel) panel.remove(); clearInterval(App.vRecordInterval); } }

async function toggleAudioRecording() { const btn = q('recordBtn'); if (App.mediaRecorder && App.mediaRecorder.state === 'recording') { stopAudioRecording(); return; } if (App.videoRecorder && App.videoRecorder.state === 'recording') { showToast('Video recording in progress'); return; } try { const stream = await navigator.mediaDevices.getUserMedia({audio: true}); App.mediaRecorder = new MediaRecorder(stream); App.recordedChunks = []; App.recordingStart = Date.now(); App.mediaRecorder.ondataavailable = (e) => { if (e.data.size > 0) App.recordedChunks.push(e.data); }; App.mediaRecorder.onstop = () => { processAudioRecording(); stream.getTracks().forEach((t) => t.stop()); }; App.mediaRecorder.start(100); btn.classList.add('recording'); showAudioRecordingPanel(true); } catch (e) { showToast('Microphone access denied'); } }
function stopAudioRecording() { if (App.mediaRecorder && App.mediaRecorder.state === 'recording') App.mediaRecorder.stop(); }
async function processAudioRecording() { const blob = new Blob(App.recordedChunks, {type: 'audio/webm'}); if (blob.size > 2.9 * 1024 * 1024) { showToast('Voice too long (max 3MB)'); q('recordBtn').classList.remove('recording'); showAudioRecordingPanel(false); return; } const arrayBuffer = await blob.arrayBuffer(); const audioCtx = new (window.AudioContext || window.webkitAudioContext)(); const audioBuffer = await audioCtx.decodeAudioData(arrayBuffer); const waveform = generateWaveform(audioBuffer); const duration = Math.floor(audioBuffer.duration); const reader = new FileReader(); reader.onload = (e) => { sendMediaMessage({text: 'Voice message', mediaType: 'voice', fileUrl: e.target.result, fileName: 'voice.webm', fileSize: blob.size, mime: 'audio/webm', duration, waveform}); q('recordBtn').classList.remove('recording'); showAudioRecordingPanel(false); }; reader.readAsDataURL(blob); }
function generateWaveform(audioBuffer) { const data = audioBuffer.getChannelData(0); const step = Math.floor(data.length / 30); let out = []; for (let i = 0; i < 30; i++) { let sum = 0; for (let j = 0; j < step; j++) sum += Math.abs(data[i * step + j]); out.push(Math.min(28, Math.max(4, Math.floor(sum / step * 80)))); } return out.join(','); }
function showAudioRecordingPanel(show) { const input = q('messageInput'), attach = q('attachBtn'), vn = q('videoNoteBtn'); if (show) { input.style.display = 'none'; attach.style.display = 'none'; vn.style.display = 'none'; const panel = document.createElement('div'); panel.id = 'recordPanel'; panel.className = 'record-panel'; panel.innerHTML = '<span class="record-timer" id="recordTimer">0:00</span><span class="cancel-record" id="cancelRecord">Cancel</span>'; q('inputArea').insertBefore(panel, q('sendBtn')); q('cancelRecord').addEventListener('click', () => { if (App.mediaRecorder) { App.recordedChunks = []; App.mediaRecorder.stop(); } q('recordBtn').classList.remove('recording'); showAudioRecordingPanel(false); }); App.recordInterval = setInterval(() => { const s = Math.floor((Date.now() - App.recordingStart) / 1000); q('recordTimer').textContent = Math.floor(s / 60) + ':' + (s % 60).toString().padStart(2, '0'); }, 1000); } else { input.style.display = ''; attach.style.display = ''; vn.style.display = ''; const panel = q('recordPanel'); if (panel) panel.remove(); clearInterval(App.recordInterval); } }

let typingTimer = null;
function onTyping() { if (!App.user) return; socket.emit('typing_start', {chatId: App.currentChatId}); clearTimeout(typingTimer); typingTimer = setTimeout(stopTyping, 1200); }
function stopTyping() { socket.emit('typing_stop', {chatId: App.currentChatId}); }
function handleTyping(data) { if (data.chatId === App.currentChatId) App.typing = data.active ? data : null; updateTypingIndicator(); }
function updateTypingIndicator() { q('typingIndicator').textContent = App.typing ? App.typing.username + ' is typing...' : ''; }

function bindProfile() {
  setupDropZone('profileDrop', 'profileAvatar', 'profileAvatarPreview', 'profileDropText', (b64) => { App.selectedAvatar = b64; }, true);
  q('profileBtn').addEventListener('click', () => { updateProfileUI(); q('profileModal').classList.remove('hidden'); });
  document.querySelectorAll('.modal-close').forEach((b) => b.addEventListener('click', () => q(b.dataset.modal).classList.add('hidden')));
  q('saveProfile').addEventListener('click', () => { socket.emit('update_profile', {about: q('profileAbout').value, avatarBase64: App.selectedAvatar}); q('profileModal').classList.add('hidden'); });
  q('logoutBtn').addEventListener('click', () => { logout(); q('profileModal').classList.add('hidden'); });
}
function updateProfileUI() { if (!App.user) return; q('profileUsername').textContent = App.user.username; q('profileAbout').value = App.user.about || ''; if (App.user.avatar) { q('profileAvatarPreview').src = App.user.avatar; q('profileAvatarPreview').style.display = 'block'; q('profileDropText').style.display = 'none'; } }
function logout() { socket.emit('logout'); localStorage.removeItem('token'); App.token = null; App.user = null; App.users = []; App.chats = []; App.currentChatId = 'global'; showAuth(); }

function bindTheme() {
  q('themeBtn').addEventListener('click', renderThemeGrid);
  document.querySelector('[data-modal="themeModal"]').addEventListener('click', () => q('themeModal').classList.add('hidden'));
}
function renderThemeGrid() {
  const grid = q('themeGrid'); grid.innerHTML = '';
  const list = [{k:'dark',n:'Dark'},{k:'light',n:'Light'},{k:'midnight',n:'Midnight'},{k:'ocean',n:'Ocean'},{k:'sunset',n:'Sunset'},{k:'matrix',n:'Matrix'},{k:'pink',n:'Sakura'},{k:'gold',n:'Gold'}];
  list.forEach((t) => { const div = document.createElement('div'); div.className = 'theme-option' + (App.theme === t.k ? ' active' : ''); div.style.background = THEMES_SERVER[t.k].accent; div.textContent = t.n; div.addEventListener('click', () => { applyTheme(t.k, true); renderThemeGrid(); }); grid.appendChild(div); });
  q('themeModal').classList.remove('hidden');
}

function bindGroup() {
  q('newGroupBtn').addEventListener('click', () => {
    const container = q('groupMembers'); container.innerHTML = '';
    App.users.forEach((u) => { if (u.id === App.user.id) return; const label = document.createElement('label'); label.className = 'member-option'; label.innerHTML = '<input type="checkbox" value="' + u.id + '"><span>' + escapeHTML(u.username) + '</span>'; container.appendChild(label); });
    q('groupModal').classList.remove('hidden');
  });
  q('tabChats').addEventListener('click', () => switchTab('chats'));
  q('tabUsers').addEventListener('click', () => switchTab('users'));
  q('createGroup').addEventListener('click', () => {
    const name = q('groupName').value.trim();
    if (!name) return showToast('Enter group name');
    const members = Array.from(q('groupMembers').querySelectorAll('input:checked')).map((i) => i.value);
    socket.emit('create_group', {name, members});
    q('groupModal').classList.add('hidden'); q('groupName').value = '';
  });
}

function bindContextMenu() {
  document.addEventListener('click', () => { q('contextMenu').classList.add('hidden'); });
}
function showContextMenu(e, msg) {
  App.contextMsg = msg;
  const menu = q('contextMenu'); menu.innerHTML = '';
  const isOwn = msg.sender && msg.sender.id === (App.user && App.user.id);
  const canEdit = isOwn && msg.type !== 'system' && !msg.deletedForAll && msg.mediaType === 'text' && (Date.now() - msg.timestamp < 48 * 60 * 60 * 1000);
  const canDelete = isOwn && !msg.deletedForAll;
  const canPin = App.currentChatId.startsWith('group:') || App.currentChatId === 'global';
  addMenuItem('📋 Copy text', () => { if (navigator.clipboard && msg.text) navigator.clipboard.writeText(msg.text).catch(() => {}); });
  addMenuItem('↩️ Reply', () => setReplyTo(msg));
  addMenuItem('➕ Reaction', () => showReactionPicker(e, msg.id));
  if (canEdit) addMenuItem('✏️ Edit', () => setEditTo(msg));
  if (canPin) addMenuItem('📌 Pin', () => socket.emit('pin_message', {chatId: App.currentChatId, messageId: msg.id}));
  if (canDelete) addMenuItem('🗑 Delete', () => { if (confirm('Delete for everyone?')) socket.emit('delete_message', {chatId: App.currentChatId, messageId: msg.id}); });
  menu.classList.remove('hidden');
  menu.style.left = Math.min(e.pageX, window.innerWidth - 180) + 'px';
  menu.style.top = Math.min(e.pageY, window.innerHeight - menu.offsetHeight - 10) + 'px';
  function addMenuItem(text, cb) { const btn = document.createElement('button'); btn.textContent = text; btn.addEventListener('click', (ev) => { ev.stopPropagation(); cb(); menu.classList.add('hidden'); }); menu.appendChild(btn); }
}
function bindReactionPicker() {
  const picker = q('reactionPicker');
  picker.querySelectorAll('span').forEach((span) => span.addEventListener('click', () => { if (App.contextReactionMsgId) socket.emit('add_reaction', {chatId: App.currentChatId, messageId: App.contextReactionMsgId, emoji: span.textContent}); picker.classList.add('hidden'); }));
  document.addEventListener('click', () => picker.classList.add('hidden'));
}
function showReactionPicker(e, msgId) {
  App.contextReactionMsgId = msgId;
  const picker = q('reactionPicker');
  picker.classList.remove('hidden');
  picker.style.left = Math.min(e.pageX, window.innerWidth - 240) + 'px';
  picker.style.top = (e.pageY - 50) + 'px';
  e.stopPropagation();
}

function bindDragDrop() {
  const overlay = q('dragOverlay'); let counter = 0;
  window.addEventListener('dragenter', (e) => { e.preventDefault(); counter++; overlay.classList.remove('hidden'); });
  window.addEventListener('dragleave', (e) => { e.preventDefault(); counter--; if (counter <= 0) { counter = 0; overlay.classList.add('hidden'); } });
  window.addEventListener('dragover', (e) => e.preventDefault());
  window.addEventListener('drop', (e) => { e.preventDefault(); counter = 0; overlay.classList.add('hidden'); if (e.dataTransfer.files && e.dataTransfer.files[0]) handleFileUpload(e.dataTransfer.files[0]); });
}

function showMediaPreview(src, type) { const preview = q('mediaPreview'); preview.innerHTML = '<button>×</button>'; const el = type === 'image' ? document.createElement('img') : document.createElement('video'); el.src = src; if (type === 'video') { el.controls = true; el.autoplay = true; } preview.appendChild(el); preview.classList.remove('hidden'); preview.querySelector('button').addEventListener('click', () => preview.classList.add('hidden')); }
function downloadDataUrl(dataUrl, filename) { const a = document.createElement('a'); a.href = dataUrl; a.download = filename; document.body.appendChild(a); a.click(); document.body.removeChild(a); }
function scrollToBottom() { q('messagesArea').scrollTop = q('messagesArea').scrollHeight; }
function formatTime(ts) { const d = new Date(ts); return d.getHours().toString().padStart(2, '0') + ':' + d.getMinutes().toString().padStart(2, '0'); }
function formatDuration(s) { if (!isFinite(s) || s < 0) s = 0; return Math.floor(s / 60) + ':' + Math.floor(s % 60).toString().padStart(2, '0'); }
function formatBytes(b) { if (b === 0) return '0 B'; const k = 1024; const sizes = ['B','KB','MB']; const i = Math.floor(Math.log(b) / Math.log(k)); return (b / Math.pow(k, i)).toFixed(1) + ' ' + sizes[i]; }
function playNotification() { try { const AC = window.AudioContext || window.webkitAudioContext; if (!AC) return; const ctx = new AC(); const osc = ctx.createOscillator(); const gain = ctx.createGain(); osc.connect(gain); gain.connect(ctx.destination); osc.type = 'sine'; osc.frequency.setValueAtTime(900, ctx.currentTime); osc.frequency.exponentialRampToValueAtTime(500, ctx.currentTime + 0.1); gain.gain.setValueAtTime(0.08, ctx.currentTime); gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.12); osc.start(); osc.stop(ctx.currentTime + 0.12); } catch (e) {} }
function showToast(msg) { const t = q('toast'); t.textContent = msg; t.classList.remove('hidden'); setTimeout(() => t.classList.add('hidden'), 3000); }
function checkMobile() { q('chatContainer').classList.toggle('mobile', window.innerWidth <= 768); }
function escapeHTML(text) { if (text == null) return ''; return String(text).replace(/[&<>"']/g, (m) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

document.addEventListener('DOMContentLoaded', init);
</script>
</body>
</html>`;

app.get('/', (req, res) => { res.send(HTML_PAGE); });

io.on('connection', (socket) => {
  socket.on('register', (data) => {
    try {
      if (!data || !data.username || !data.password) return socket.emit('register_error', 'Username and password required');
      const username = String(data.username).trim().toLowerCase();
      const password = String(data.password);
      if (username.length < 3 || username.length > 30) return socket.emit('register_error', 'Username 3-30 chars');
      if (password.length < 4) return socket.emit('register_error', 'Password min 4 chars');
      for (const u of users.values()) if (u.username === username) return socket.emit('register_error', 'Username taken');
      if (data.avatarBase64 && data.avatarBase64.length > MAX_AVATAR_BASE64_LEN) return socket.emit('register_error', 'Avatar too large');
      const salt = crypto.randomBytes(16).toString('hex');
      const id = uuidv4();
      const user = { id, username, passwordHash: hashPassword(password, salt), salt, avatarBase64: data.avatarBase64 || null, about: escapeHTML(data.about || ''), theme: data.theme || 'dark', socketId: null, lastSeen: Date.now() };
      users.set(id, user);
      if (!adminId) adminId = id;
      ensureGlobalChat();
      const token = generateToken();
      sessions.set(token, id);
      setUserOnline(user, socket);
      socket.emit('logged_in', { user: getUserPublicProfile(user), token });
      socket.emit('chats_list', getChatListForUser(id));
      socket.emit('unread_counts', getUnreadCountsForUser(id));
      addSystemMessage('global', username + ' joined');
      broadcastUsers();
    } catch (e) { console.error(e); socket.emit('register_error', 'Server error'); }
  });

  socket.on('login', (data) => {
    try {
      if (!data || !data.username || !data.password) return socket.emit('login_error', 'Invalid credentials');
      const username = String(data.username).trim().toLowerCase();
      const user = Array.from(users.values()).find((u) => u.username === username);
      if (!user) return socket.emit('login_error', 'Invalid credentials');
      if (hashPassword(String(data.password), user.salt) !== user.passwordHash) return socket.emit('login_error', 'Invalid credentials');
      ensureGlobalChat();
      const wasOnline = onlineSockets.has(user.id) && onlineSockets.get(user.id).size > 0;
      const token = generateToken();
      sessions.set(token, user.id);
      setUserOnline(user, socket);
      socket.emit('logged_in', { user: getUserPublicProfile(user), token });
      socket.emit('chats_list', getChatListForUser(user.id));
      socket.emit('unread_counts', getUnreadCountsForUser(user.id));
      broadcastUsers();
      if (!wasOnline) addSystemMessage('global', user.username + ' joined');
    } catch (e) { console.error(e); socket.emit('login_error', 'Server error'); }
  });

  socket.on('authenticate', (data) => {
    try {
      if (!data || !data.token) return socket.emit('auth_error');
      const userId = sessions.get(data.token);
      if (!userId) return socket.emit('auth_error');
      const user = users.get(userId);
      if (!user) return socket.emit('auth_error');
      ensureGlobalChat();
      const wasOnline = onlineSockets.has(user.id) && onlineSockets.get(user.id).size > 0;
      setUserOnline(user, socket);
      socket.emit('logged_in', { user: getUserPublicProfile(user), token: data.token });
      socket.emit('chats_list', getChatListForUser(user.id));
      socket.emit('unread_counts', getUnreadCountsForUser(user.id));
      broadcastUsers();
      if (!wasOnline) addSystemMessage('global', user.username + ' joined');
    } catch (e) { console.error(e); socket.emit('auth_error'); }
  });

  socket.on('logout', () => {
    const userId = socket.data.userId;
    if (userId) {
      for (const [t, uid] of sessions.entries()) if (uid === userId) sessions.delete(t);
      setUserOffline(socket);
      broadcastUsers();
    }
    socket.emit('logged_out');
  });

  socket.on('get_users', () => { if (!socket.data.userId) return; socket.emit('users_list', Array.from(users.values()).map(getUserPublicProfile)); });
  socket.on('get_chats', () => { if (!socket.data.userId) return; socket.emit('chats_list', getChatListForUser(socket.data.userId)); socket.emit('unread_counts', getUnreadCountsForUser(socket.data.userId)); });

  socket.on('send_message', (data) => {
    try {
      const senderId = socket.data.userId;
      if (!senderId) return;
      const sender = users.get(senderId);
      if (!sender) return;
      const chat = getChat(data.chatId);
      if (!chat) return;
      if (chat.type !== 'global' && !chat.members.has(senderId)) return;
      const mediaType = data.mediaType || 'text';
      let text = String(data && data.text || '').trim();
      if (mediaType === 'text' && !text) return;
      if (text.length > MESSAGE_MAX_LEN) text = text.slice(0, MESSAGE_MAX_LEN);
      if (mediaType === 'text') text = escapeHTML(text);
      if (data.fileUrl && data.fileUrl.length > MAX_FILE_BASE64_LEN) return socket.emit('error_message', 'File too large');
      const payload = { text, mediaType, fileUrl: data.fileUrl || null, fileName: data.fileName ? escapeHTML(String(data.fileName)) : null, fileSize: data.fileSize || 0, mime: data.mime || null, duration: data.duration || null, waveform: data.waveform || null, replyTo: data.replyTo || null };
      const msg = storeMessage(chat, senderId, payload);
      broadcastChatMessage(chat, msg);
      broadcastChatUpdate(chat, 'last_message', { lastMessage: enrichMessage(msg) });
    } catch (e) { console.error(e); }
  });

  socket.on('edit_message', (data) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const chat = getChat(data.chatId);
    if (!chat) return;
    const msg = chat.messages.find((m) => m.id === data.messageId);
    if (!msg || msg.senderId !== userId || msg.type === 'system' || msg.deletedForAll) return;
    if (Date.now() - msg.timestamp > EDIT_WINDOW_MS) return socket.emit('error_message', 'Edit time expired');
    msg.text = escapeHTML(String(data.text || '').trim().slice(0, MESSAGE_MAX_LEN));
    msg.editedAt = Date.now();
    const full = enrichMessage(msg);
    broadcastChatUpdate(chat, 'message_edited', { messageId: msg.id, message: full, lastMessage: full });
  });

  socket.on('delete_message', (data) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const chat = getChat(data.chatId);
    if (!chat) return;
    const msg = chat.messages.find((m) => m.id === data.messageId);
    if (!msg || msg.senderId !== userId || msg.type === 'system') return;
    msg.deletedForAll = true;
    msg.text = '';
    msg.fileUrl = null;
    msg.mediaType = 'text';
    const full = enrichMessage(msg);
    broadcastChatUpdate(chat, 'message_deleted', { messageId: msg.id, message: full, lastMessage: full });
  });

  socket.on('add_reaction', (data) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const chat = getChat(data.chatId);
    if (!chat) return;
    const msg = chat.messages.find((m) => m.id === data.messageId);
    if (!msg || msg.type === 'system' || msg.deletedForAll) return;
    const emoji = String(data.emoji).trim();
    if (!emoji) return;
    msg.reactions = msg.reactions || {};
    msg.reactions[emoji] = msg.reactions[emoji] || [];
    const idx = msg.reactions[emoji].indexOf(userId);
    if (idx >= 0) msg.reactions[emoji].splice(idx, 1);
    else msg.reactions[emoji].push(userId);
    if (msg.reactions[emoji].length === 0) delete msg.reactions[emoji];
    const full = enrichMessage(msg);
    broadcastChatUpdate(chat, 'reactions_updated', { messageId: msg.id, message: full, lastMessage: full });
  });

  socket.on('pin_message', (data) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const chat = getChat(data.chatId);
    if (!chat) return;
    if (chat.type === 'private') return socket.emit('error_message', 'Pin only in groups/global');
    if (chat.type === 'group' && !isGroupAdmin(chat, userId)) return socket.emit('error_message', 'Only admin can pin');
    const msg = data.messageId ? chat.messages.find((m) => m.id === data.messageId) : null;
    chat.pinnedId = msg ? msg.id : null;
    broadcastChatUpdate(chat, 'pinned_changed', { pinnedId: chat.pinnedId, pinnedText: msg ? (msg.text || (msg.fileName || 'Media')) : '' });
  });

  socket.on('typing_start', (data) => {
    const senderId = socket.data.userId;
    if (!senderId) return;
    const sender = users.get(senderId);
    if (!sender) return;
    const chat = getChat(data.chatId);
    if (!chat) return;
    const payload = { chatId: chat.id, userId: senderId, username: sender.username, active: true };
    if (chat.type === 'global') socket.broadcast.emit('typing', payload);
    else chat.members.forEach((uid) => { if (uid !== senderId) broadcastToUser(uid, 'typing', payload); });
  });

  socket.on('typing_stop', (data) => {
    const senderId = socket.data.userId;
    if (!senderId) return;
    const sender = users.get(senderId);
    if (!sender) return;
    const chat = getChat(data.chatId);
    if (!chat) return;
    const payload = { chatId: chat.id, userId: senderId, username: sender.username, active: false };
    if (chat.type === 'global') socket.broadcast.emit('typing', payload);
    else chat.members.forEach((uid) => { if (uid !== senderId) broadcastToUser(uid, 'typing', payload); });
  });

  socket.on('get_history', (data) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const chat = getChat(data.chatId);
    if (!chat) return socket.emit('history', { chatId: data.chatId, messages: [], pinnedId: null });
    if (chat.type !== 'global' && !chat.members.has(userId)) return socket.emit('history', { chatId: data.chatId, messages: [], pinnedId: null });
    markMessagesRead(chat.id, userId);
    socket.emit('history', { chatId: chat.id, pinnedId: chat.pinnedId, messages: chat.messages.map(enrichMessage) });
  });

  socket.on('get_unread_counts', () => {
    const userId = socket.data.userId;
    if (!userId) return;
    const counts = {};
    for (const chat of chats.values()) {
      counts[chat.id] = getUnreadCount(userId, chat.id);
    }
    socket.emit('unread_counts', counts);
  });

  socket.on('update_profile', (data) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const user = users.get(userId);
    if (!user) return;
    if (data.about !== undefined) user.about = escapeHTML(String(data.about).slice(0, 140));
    if (data.avatarBase64 !== undefined) {
      if (data.avatarBase64 && data.avatarBase64.length > MAX_AVATAR_BASE64_LEN) return socket.emit('profile_error', 'Avatar too large');
      user.avatarBase64 = data.avatarBase64 || user.avatarBase64;
    }
    if (data.theme !== undefined && THEMES[data.theme]) user.theme = data.theme;
    broadcastUsers();
    socket.emit('profile_updated', { user: getUserPublicProfile(user) });
  });

  socket.on('create_group', (data) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const creator = users.get(userId);
    if (!creator) return;
    const validMembers = (data.members || []).filter((id) => users.has(id) && id !== userId).slice(0, 49);
    const chat = createGroup(data.name, userId, validMembers);
    addSystemMessage(chat.id, 'Group "' + chat.name + '" created');
    broadcastChatUpdate(chat, 'chat_created', {});
    socket.emit('chats_list', getChatListForUser(userId));
  });

  socket.on('open_private_chat', (data, callback) => {
    const userId = socket.data.userId;
    if (!userId) return;
    const otherUser = users.get(data.otherUserId);
    if (!otherUser) return;
    const chat = ensurePrivateChat(userId, data.otherUserId);
    const chatData = {
      id: chat.id,
      type: 'private',
      userId: data.otherUserId,
      name: otherUser.username,
      avatar: otherUser.avatarBase64,
      lastMessage: chat.messages[chat.messages.length - 1] || null,
      unread: getUnreadCount(userId, chat.id)
    };
    if (callback) callback(chatData);
  });

  socket.on('clear_history', () => {
    const userId = socket.data.userId;
    if (!userId) return;
    if (userId !== adminId) return socket.emit('error_message', 'Only admin can clear history');
    const chat = getChat('global');
    if (chat) { chat.messages.length = 0; chat.pinnedId = null; }
    io.emit('history_cleared', { chatId: 'global' });
  });

  socket.on('disconnect', () => {
    setUserOffline(socket);
    broadcastUsers();
  });
});

server.listen(PORT, () => console.log('Server listening on port ' + PORT));
