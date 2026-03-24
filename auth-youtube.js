require('dotenv').config();
const http     = require('http');
const fs       = require('fs');
const { exec } = require('child_process');

const CLIENT_ID     = process.env.GOOGLE_CLIENT_ID;
const CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const REDIRECT_URI  = 'http://localhost:8080';
const SCOPES = [
  'https://www.googleapis.com/auth/youtube.readonly',
  'https://www.googleapis.com/auth/yt-analytics.readonly',
].join(' ');

const authUrl = new URL('https://accounts.google.com/o/oauth2/auth');
authUrl.searchParams.set('client_id',     CLIENT_ID);
authUrl.searchParams.set('redirect_uri',  REDIRECT_URI);
authUrl.searchParams.set('response_type', 'code');
authUrl.searchParams.set('scope',         SCOPES);
authUrl.searchParams.set('access_type',   'offline');
authUrl.searchParams.set('prompt',        'consent'); // force refresh_token to be returned

console.log('\n YouTube Analytics — One-Time Auth Setup');
console.log('==========================================');
console.log('\nOpening your browser...');
console.log('If it does not open, paste this URL manually:\n');
console.log(authUrl.toString());
console.log('\nWaiting for you to approve in the browser...\n');

// Open browser (Windows)
exec(`powershell.exe -Command "Start-Process '${authUrl.toString()}'"`, () => {});

const server = http.createServer(async (req, res) => {
  const params = new URL(req.url, 'http://localhost:8080').searchParams;
  const code   = params.get('code');
  const error  = params.get('error');

  if (error) {
    res.writeHead(400, { 'Content-Type': 'text/html' });
    res.end(`<html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#f87171"><h2>Auth denied: ${error}</h2></body></html>`);
    server.close(() => process.exit(1));
    return;
  }

  if (!code) { res.end('Waiting...'); return; }

  // Exchange code → tokens
  const tokenRes = await fetch('https://oauth2.googleapis.com/token', {
    method:  'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id:     CLIENT_ID,
      client_secret: CLIENT_SECRET,
      redirect_uri:  REDIRECT_URI,
      code,
      grant_type: 'authorization_code',
    }),
  });

  const tokens = await tokenRes.json();

  if (tokens.error) {
    console.error('Token error:', tokens);
    res.writeHead(400, { 'Content-Type': 'text/html' });
    res.end(`<html><body style="font-family:sans-serif;padding:40px;background:#0f172a;color:#f87171"><h2>Error: ${tokens.error_description}</h2></body></html>`);
    server.close(() => process.exit(1));
    return;
  }

  // Save refresh token to .env
  let env = fs.readFileSync('.env', 'utf8');
  if (env.includes('GOOGLE_REFRESH_TOKEN=')) {
    env = env.replace(/GOOGLE_REFRESH_TOKEN=.*/, `GOOGLE_REFRESH_TOKEN=${tokens.refresh_token}`);
  } else {
    env += `\nGOOGLE_REFRESH_TOKEN=${tokens.refresh_token}`;
  }
  fs.writeFileSync('.env', env.trimEnd() + '\n');

  res.writeHead(200, { 'Content-Type': 'text/html' });
  res.end(`<html><body style="font-family:sans-serif;text-align:center;padding:80px;background:#080d14;color:#4ade80">
    <svg width="64" height="64" fill="none" stroke="#4ade80" stroke-width="2" viewBox="0 0 24 24" style="margin-bottom:20px">
      <circle cx="12" cy="12" r="10"/><path d="m9 12 2 2 4-4"/>
    </svg>
    <h1 style="font-size:24px;margin-bottom:8px;">Authorization Complete!</h1>
    <p style="color:#64748b;font-size:14px;">Refresh token saved. You can close this tab.</p>
  </body></html>`);

  console.log('✓ Refresh token saved to .env');
  console.log('\nRunning first YouTube Analytics sync...\n');

  server.close(() => {
    // Trigger a sync now that we have the token
    require('child_process').execSync('node sync.js --once', { stdio: 'inherit', cwd: __dirname });
    process.exit(0);
  });
});

server.listen(8080, () => {
  console.log('Listening on http://localhost:8080 for Google callback...');
});
