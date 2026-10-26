const fs = require('fs');
const path = require('path');

function getEmails() {
  let raw = process.env.ALERT_EMAILS || process.env.ALERT_EMAIL || '';

  // Fallback: If not already in process.env, read from .envs/sls.env
  if (!raw) {
    const envCandidates = [
      path.resolve(__dirname, '../../../.envs/sls.env'),
      path.resolve(__dirname, '../../../../.envs/sls.env'),
      path.resolve(process.cwd(), '.envs/sls.env'),
    ];

    for (const envPath of envCandidates) {
      if (fs.existsSync(envPath)) {
        const content = fs.readFileSync(envPath, 'utf8');
        for (const line of content.split('\n')) {
          const trimmed = line.trim();
          if (trimmed.startsWith('ALERT_EMAILS=') || trimmed.startsWith('ALERT_EMAIL=')) {
            raw = trimmed.substring(trimmed.indexOf('=') + 1);
            break;
          }
        }
        if (raw) break;
      }
    }
  }

  if (!raw) return [];

  return raw
    .split(',')
    .map((e) => e.trim().replace(/^['"]|['"]$/g, ''))
    .filter(Boolean);
}

module.exports = () => {
  const emails = getEmails();

  const Resources = {
    SocAlertsTopic: {
      Type: 'AWS::SNS::Topic',
      Properties: {
        TopicName: 'nexasecure-soc-alerts-${self:provider.stage}',
        DisplayName: 'NexaSecure WIDS SOC Alerts',
      },
    },
  };

  emails.forEach((email, index) => {
    Resources[`SocAlertsEmailSubscription${index + 1}`] = {
      Type: 'AWS::SNS::Subscription',
      Properties: {
        TopicArn: { Ref: 'SocAlertsTopic' },
        Protocol: 'email',
        Endpoint: email,
      },
    };
  });

  return { Resources };
};

