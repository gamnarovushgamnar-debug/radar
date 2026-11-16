const { app, BrowserWindow, shell } = require('electron');
const path = require('path');

// Адреса вашого сервера (Render). Можна перевизначити змінною RADAR_URL.
const RADAR_URL = process.env.RADAR_URL || 'https://YOUR-APP.onrender.com';

function createWindow() {
  const win = new BrowserWindow({
    width: 1280,
    height: 800,
    minWidth: 800,
    minHeight: 500,
    backgroundColor: '#0b0f14',
    title: 'Радар цілей',
    icon: path.join(__dirname, 'icon.png'),
    autoHideMenuBar: true,
    webPreferences: { backgroundThrottling: false }   // карта не «засипає» у фоні
  });

  win.loadURL(RADAR_URL);

  // зовнішні посилання відкриваємо в браузері, а не всередині радару
  win.webContents.setWindowOpenHandler(({ url }) => { shell.openExternal(url); return { action: 'deny' }; });
}

app.whenReady().then(createWindow);
app.on('activate', () => { if (BrowserWindow.getAllWindows().length === 0) createWindow(); });
app.on('window-all-closed', () => app.quit());
