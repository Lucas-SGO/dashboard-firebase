const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = Number(process.env.PORT || 5173);
const SITE_DIR = path.join(__dirname, 'site');

const mimeTypes = {
  '.html': 'text/html; charset=UTF-8',
  '.css': 'text/css; charset=UTF-8',
  '.js': 'application/javascript; charset=UTF-8',
  '.json': 'application/json; charset=UTF-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.ico': 'image/x-icon'
};

function sendFile(filePath, response) {
  fs.readFile(filePath, (error, data) => {
    if (error) {
      response.writeHead(404, { 'Content-Type': 'text/plain; charset=UTF-8' });
      response.end('Arquivo nao encontrado');
      return;
    }

    const ext = path.extname(filePath).toLowerCase();
    response.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'application/octet-stream' });
    response.end(data);
  });
}

const server = http.createServer((request, response) => {
  const requestPath = decodeURIComponent(request.url.split('?')[0]);
  const safePath = path.normalize(requestPath).replace(/^\/+/, '');
  let filePath = path.join(SITE_DIR, safePath || 'index.html');

  if (!filePath.startsWith(SITE_DIR)) {
    response.writeHead(403, { 'Content-Type': 'text/plain; charset=UTF-8' });
    response.end('Acesso negado');
    return;
  }

  fs.stat(filePath, (error, stats) => {
    if (!error && stats.isDirectory()) {
      filePath = path.join(filePath, 'index.html');
    }

    sendFile(filePath, response);
  });
});

server.listen(PORT, () => {
  console.log(`dashboard_firebase rodando em http://localhost:${PORT}`);
});
