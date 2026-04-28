module.exports = {
  apps: [
    {
      name: 'ycbot-ws-relay',
      script: 'app.js',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      max_memory_restart: '200M',
      env: {
        NODE_ENV: 'production',
        PORT: 8080,
        LOG_LEVEL: 'info',
        // Per-user auth: relay loads valid tokens from Firestore at startup
        // (collection: relay_auth_tokens) and live-updates via onSnapshot.
        // The relay VM's attached service account must have roles/datastore.user
        // on FIREBASE_PROJECT_ID. No shared-secret env var needed.
        FIREBASE_PROJECT_ID: 'ycbot-6f336',
      },
      error_file: './logs/err.log',
      out_file: './logs/out.log',
      log_date_format: 'YYYY-MM-DDTHH:mm:ss',
      merge_logs: true,
    },
  ],
};
