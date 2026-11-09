// Tópico SNS de operación: recibe las alarmas de CloudWatch, los fallos de Glue/EMR
// (EventBridge) y los avisos de presupuesto. Se mantiene separado del tópico del SOC
// (nexasecure-soc-alerts) para no mezclar "el pipeline falló" con "hay un ataque".
const fs = require('fs');
const path = require('path');

const EMAIL_KEYS = ['OPS_ALERT_EMAILS', 'ALERT_EMAILS', 'ALERT_EMAIL'];

function readFromEnvFile() {
  const candidates = [
    path.resolve(__dirname, '../../../.envs/sls.env'),
    path.resolve(process.cwd(), '.envs/sls.env'),
  ];
  for (const envPath of candidates) {
    if (!fs.existsSync(envPath)) continue;
    const lines = fs.readFileSync(envPath, 'utf8').split('\n');
    for (const key of EMAIL_KEYS) {
      const line = lines.map((l) => l.trim()).find((l) => l.startsWith(`${key}=`));
      if (line) return line.substring(line.indexOf('=') + 1);
    }
  }
  return '';
}

function getEmails() {
  const fromEnv = EMAIL_KEYS.map((key) => process.env[key]).find(Boolean);
  const raw = fromEnv || readFromEnvFile();
  return raw
    .split(',')
    .map((e) => e.trim().replace(/^['"]|['"]$/g, ''))
    .filter(Boolean);
}

module.exports = () => {
  const accountId = '${aws:accountId}';
  const region = '${aws:region}';

  const Resources = {
    OpsAlertsTopic: {
      Type: 'AWS::SNS::Topic',
      Properties: {
        TopicName: 'nexasecure-ops-alerts-${self:provider.stage}',
        DisplayName: 'NexaSecure WIDS OPS',
      },
    },
    // Al declarar una política propia se reemplaza la política por defecto del tópico,
    // así que se listan de forma explícita todos los publicadores permitidos.
    OpsAlertsTopicPolicy: {
      Type: 'AWS::SNS::TopicPolicy',
      Properties: {
        Topics: [{ Ref: 'OpsAlertsTopic' }],
        PolicyDocument: {
          Version: '2012-10-17',
          Statement: [
            {
              Sid: 'AccountOwnerAccess',
              Effect: 'Allow',
              Principal: { AWS: `arn:aws:iam::${accountId}:root` },
              Action: [
                'SNS:GetTopicAttributes',
                'SNS:SetTopicAttributes',
                'SNS:AddPermission',
                'SNS:RemovePermission',
                'SNS:DeleteTopic',
                'SNS:Subscribe',
                'SNS:ListSubscriptionsByTopic',
                'SNS:Publish',
              ],
              Resource: { Ref: 'OpsAlertsTopic' },
            },
            {
              Sid: 'AllowCloudWatchAlarms',
              Effect: 'Allow',
              Principal: { Service: 'cloudwatch.amazonaws.com' },
              Action: 'SNS:Publish',
              Resource: { Ref: 'OpsAlertsTopic' },
              Condition: {
                ArnLike: { 'aws:SourceArn': `arn:aws:cloudwatch:${region}:${accountId}:alarm:*` },
              },
            },
            {
              Sid: 'AllowEventBridgeRules',
              Effect: 'Allow',
              Principal: { Service: 'events.amazonaws.com' },
              Action: 'SNS:Publish',
              Resource: { Ref: 'OpsAlertsTopic' },
              Condition: {
                ArnLike: { 'aws:SourceArn': `arn:aws:events:${region}:${accountId}:rule/nexasecure-*` },
              },
            },
            {
              Sid: 'AllowBudgets',
              Effect: 'Allow',
              Principal: { Service: 'budgets.amazonaws.com' },
              Action: 'SNS:Publish',
              Resource: { Ref: 'OpsAlertsTopic' },
              Condition: {
                StringEquals: { 'aws:SourceAccount': accountId },
              },
            },
          ],
        },
      },
    },
  };

  getEmails().forEach((email, index) => {
    Resources[`OpsAlertsEmailSubscription${index + 1}`] = {
      Type: 'AWS::SNS::Subscription',
      Properties: {
        TopicArn: { Ref: 'OpsAlertsTopic' },
        Protocol: 'email',
        Endpoint: email,
      },
    };
  });

  return { Resources };
};
