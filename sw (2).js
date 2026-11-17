// Service worker радара: push-тривоги при закритому застосунку.
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
self.addEventListener('fetch', () => {});

self.addEventListener('push', e => {
  let d = {};
  try { d = e.data ? e.data.json() : {}; } catch (_) { d = { title: '🚨 ТРИВОГА', body: e.data ? e.data.text() : '' }; }
  const opts = {
    body: d.body || '',
    tag: d.tag || 'radar-alarm',
    renotify: true,
    requireInteraction: true,          // не зникає, поки не торкнешся
    silent: false,
    vibrate: [800, 200, 800, 200, 800, 200, 800, 200, 800, 200, 800],
    icon: '/icon-192.png',
    badge: '/icon-192.png',
    data: d,
    actions: [{ action: 'open', title: 'Відкрити радар' }]
  };
  e.waitUntil((async () => {
    // якщо радар відкритий (навіть у фоні) — будимо там повну сирену
    const wins = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    wins.forEach(c => c.postMessage({ type: 'alarm', payload: d }));
    await self.registration.showNotification(d.title || '🚨 ТРИВОГА', opts);
  })());
});

self.addEventListener('notificationclick', e => {
  e.notification.close();
  const d = e.notification.data || {};
  e.waitUntil((async () => {
    const wins = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    if (wins.length) {
      const c = wins[0];
      try { await c.focus(); } catch (_) {}
      c.postMessage({ type: 'alarm', payload: d });
    } else {
      await self.clients.openWindow('/?alarm=' + encodeURIComponent(d.id || '1') + '&lvl=' + (d.lvl || 2));
    }
  })());
});
