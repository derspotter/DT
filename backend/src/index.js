import { WebSocketServer } from 'ws';
import { createApp } from './app.js';

const port = process.env.PORT || 4000;
const clients = new Set();

function broadcast(message) {
  const timestamp = new Date().toISOString();
  const formattedMessage = `[${timestamp}] ${message}`;
  clients.forEach((client) => {
    if (client.readyState === client.OPEN) {
      client.send(formattedMessage);
    }
  });
}

const app = createApp({ broadcast });
const server = app.listen(port, () => {
  console.log(`HTTP server listening on port ${port}`);
});

const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('Client connected via WebSocket');
  clients.add(ws);

  ws.on('message', (message) => {
    console.log('Received message from client:', message);
  });

  ws.on('close', () => {
    console.log('Client disconnected');
    clients.delete(ws);
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
    clients.delete(ws);
  });

  ws.send('WebSocket connection established. Waiting for script output...');
});
