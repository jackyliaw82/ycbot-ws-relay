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
      },
      error_file: './logs/err.log',
      out_file: './logs/out.log',
      log_date_format: 'YYYY-MM-DDTHH:mm:ss',
      merge_logs: true,
    },
  ],
};
