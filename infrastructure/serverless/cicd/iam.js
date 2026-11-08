// Rol que asume GitHub Actions mediante OIDC: el workflow obtiene credenciales temporales
// de AWS sin guardar llaves en GitHub. Solo el workflow que corre en las ramas de despliegue
// (main y dev) de los repositorios listados puede asumirlo, y sus permisos se limitan a desplegar el código
// de NexaSecure (Lambda de alertas, código del detector y scripts de Glue).
const OIDC_HOST = 'token.actions.githubusercontent.com';

// Repositorio original del equipo y fork de Luis. Se puede reemplazar con la variable de
// entorno GITHUB_REPOS (owner/repo separados por coma). Se lee aquí y no en serverless.yml
// porque Serverless v4 separa por comas los valores por defecto de ${env:...}.
const DEFAULT_GITHUB_REPOS = [
  'CesarMontoyag1/Proyecto_Ingenieria_datos',
  'luisNP21/Proyecto_Ingenieria_datos',
];
const DEFAULT_DEPLOY_BRANCHES = ['main', 'dev'];

// Los demás valores se dejan como variables `${...}` dentro de los textos y Serverless
// los resuelve después.
module.exports = () => {
  const accountId = '${aws:accountId}';
  const region = '${aws:region}';
  const stage = '${self:provider.stage}';
  const custom = {
    roleName: '${self:custom.roleName}',
    rawBucketName: '${self:custom.rawBucketName}',
    alertFormatterName: '${self:custom.alertFormatterName}',
  };

  const repos = (process.env.GITHUB_REPOS ? process.env.GITHUB_REPOS.split(',') : DEFAULT_GITHUB_REPOS)
    .map((r) => r.trim())
    .filter(Boolean);
  if (repos.length === 0) {
    throw new Error('GITHUB_REPOS no puede estar vacío (formato owner/repo).');
  }
  // Ramas desde las que el workflow puede desplegar (variable DEPLOY_BRANCHES, separadas por coma).
  const branches = (process.env.DEPLOY_BRANCHES ? process.env.DEPLOY_BRANCHES.split(',') : DEFAULT_DEPLOY_BRANCHES)
    .map((b) => b.trim())
    .filter(Boolean);
  const subjects = repos.flatMap((repo) => branches.map((branch) => `repo:${repo}:ref:refs/heads/${branch}`));

  const providerArn = `arn:aws:iam::${accountId}:oidc-provider/${OIDC_HOST}`;
  const rawBucketArn = `arn:aws:s3:::${custom.rawBucketName}`;

  return {
    Conditions: {
      CreateOidcProvider: { 'Fn::Equals': ['${self:custom.createOidcProvider}', 'true'] },
    },
    Resources: {
      GitHubOidcProvider: {
        Type: 'AWS::IAM::OIDCProvider',
        Condition: 'CreateOidcProvider',
        Properties: {
          Url: `https://${OIDC_HOST}`,
          ClientIdList: ['sts.amazonaws.com'],
          ThumbprintList: [
            '6938fd4d98bab03faadb97b34396831e3780aea1',
            '1c58a3a8518e8759bf075b76b750d4f2df264fcd',
          ],
        },
      },
      GitHubActionsRole: {
        Type: 'AWS::IAM::Role',
        Properties: {
          RoleName: custom.roleName,
          Description: 'Despliegue continuo de NexaSecure WIDS desde GitHub Actions (OIDC)',
          MaxSessionDuration: 3600,
          AssumeRolePolicyDocument: {
            Version: '2012-10-17',
            Statement: [
              {
                Effect: 'Allow',
                Principal: {
                  Federated: {
                    'Fn::If': ['CreateOidcProvider', { Ref: 'GitHubOidcProvider' }, providerArn],
                  },
                },
                Action: 'sts:AssumeRoleWithWebIdentity',
                Condition: {
                  StringEquals: { [`${OIDC_HOST}:aud`]: 'sts.amazonaws.com' },
                  StringLike: { [`${OIDC_HOST}:sub`]: subjects },
                },
              },
            ],
          },
          Policies: [
            {
              PolicyName: 'nexasecure-cicd-deploy',
              PolicyDocument: {
                Version: '2012-10-17',
                Statement: [
                  {
                    Sid: 'ReadNexaSecureParameters',
                    Effect: 'Allow',
                    Action: ['ssm:GetParameter'],
                    Resource: `arn:aws:ssm:${region}:${accountId}:parameter/nexasecure/${stage}/*`,
                  },
                  {
                    Sid: 'DeployAlertFormatterCode',
                    Effect: 'Allow',
                    Action: [
                      'lambda:UpdateFunctionCode',
                      'lambda:GetFunction',
                      'lambda:GetFunctionConfiguration',
                      'lambda:InvokeFunction',
                    ],
                    Resource: `arn:aws:lambda:${region}:${accountId}:function:${custom.alertFormatterName}`,
                  },
                  {
                    Sid: 'ListCodePrefixes',
                    Effect: 'Allow',
                    Action: ['s3:ListBucket'],
                    Resource: rawBucketArn,
                    Condition: {
                      StringLike: { 's3:prefix': ['flink-app/*', 'glue-jobs/*', 'flink-app', 'glue-jobs'] },
                    },
                  },
                  {
                    Sid: 'UploadCode',
                    Effect: 'Allow',
                    Action: ['s3:GetObject', 's3:PutObject'],
                    Resource: [`${rawBucketArn}/flink-app/*`, `${rawBucketArn}/glue-jobs/*`],
                  },
                  {
                    Sid: 'RestartDetectorStep',
                    Effect: 'Allow',
                    Action: [
                      'elasticmapreduce:DescribeCluster',
                      'elasticmapreduce:ListSteps',
                      'elasticmapreduce:AddJobFlowSteps',
                      'elasticmapreduce:CancelSteps',
                    ],
                    Resource: `arn:aws:elasticmapreduce:${region}:${accountId}:cluster/*`,
                  },
                ],
              },
            },
          ],
          // Sin Tags propios: el despliegue ya etiqueta los recursos del stack, y repetir una
          // clave (IAM no distingue mayúsculas, p. ej. Stage/STAGE) hace fallar la creación del rol.
        },
      },
      GitHubActionsRoleArnParameter: {
        Type: 'AWS::SSM::Parameter',
        Properties: {
          Name: `/nexasecure/${stage}/cicd/GitHubActionsRoleArn`,
          Type: 'String',
          Value: { 'Fn::GetAtt': ['GitHubActionsRole', 'Arn'] },
          Description: 'ARN del rol que asume GitHub Actions para desplegar NexaSecure',
        },
      },
    },
    Outputs: {
      GitHubActionsRoleArn: {
        Value: { 'Fn::GetAtt': ['GitHubActionsRole', 'Arn'] },
      },
    },
  };
};
