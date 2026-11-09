// Dashboard de consumo de NexaSecure WIDS en Amazon QuickSight, definido como código.
//
// Crea: rol IAM que QuickSight usa para Athena (solo el bucket Trusted), fuente de datos
// Athena, 4 datasets (uno por vista vw_*: alertas en consulta directa, tráfico en SPICE con
// refresco programado) y el dashboard "NexaSecure WIDS - SOC" con KPIs, gráficas y tablas.
//
// Requisito previo (una vez por cuenta): estar suscrito a QuickSight. Ver README.md.
const fs = require('fs');
const path = require('path');

const ACCOUNT = '${aws:accountId}';
const REGION = '${aws:region}';
const STAGE = '${self:provider.stage}';
const DATABASE = '${self:custom.catalogDatabase}';
const WORKGROUP = '${self:custom.athenaWorkGroup}';
const TRUSTED = '${self:custom.trustedBucket}';
const TIMEZONE = '${self:custom.refreshTimeZone}';

// ---------------------------------------------------------------------------
// Usuario dueño (QUICKSIGHT_USER_ARN en el entorno o en .envs/sls.env)
// ---------------------------------------------------------------------------
function readUserArn() {
  if (process.env.QUICKSIGHT_USER_ARN) return process.env.QUICKSIGHT_USER_ARN.trim();
  const envFile = path.resolve(__dirname, '../../../.envs/sls.env');
  if (fs.existsSync(envFile)) {
    const line = fs.readFileSync(envFile, 'utf8').split('\n').map((l) => l.trim())
      .find((l) => l.startsWith('QUICKSIGHT_USER_ARN='));
    if (line) return line.substring(line.indexOf('=') + 1).replace(/^['"]|['"]$/g, '').trim();
  }
  return '';
}

// ---------------------------------------------------------------------------
// Datasets: columnas de cada vista (ver consumption/sql/serving_views.sql)
// ---------------------------------------------------------------------------
//
// live: true -> DIRECT_QUERY. El dashboard consulta Athena cada vez que se abre o se
// actualiza, así que las alertas nuevas aparecen sin refrescos ni pasos manuales.
// live: false -> SPICE con refresco programado (el tráfico batch solo cambia con la ingesta).
const DATASETS = {
  alerts: {
    view: 'vw_alerts', name: 'NexaSecure - Alertas (en vivo)', live: true,
    columns: {
      alert_id: 'STRING', rule_type: 'STRING', severity: 'STRING', severity_rank: 'INTEGER',
      is_high_or_critical: 'INTEGER', bssid_pseudo: 'STRING', src_mac_pseudo: 'STRING',
      observed_count: 'INTEGER', threshold: 'INTEGER', window_seconds: 'DECIMAL',
      detected_at: 'DATETIME', detection_latency_ms: 'DECIMAL', sla_status: 'STRING',
      sla_ok_pct: 'INTEGER', detected_date: 'DATETIME',
    },
  },
  hourly: {
    view: 'vw_alerts_hourly', name: 'NexaSecure - Alertas por hora (en vivo)', live: true,
    columns: {
      detected_hour: 'DATETIME', rule_type: 'STRING', severity: 'STRING', alerts: 'INTEGER',
      affected_bssids: 'INTEGER', latency_p50_ms: 'DECIMAL', latency_p95_ms: 'DECIMAL',
      max_observed_count: 'INTEGER',
    },
  },
  traffic: {
    view: 'vw_traffic_daily', name: 'NexaSecure - Tráfico diario', live: false, refresh: 'DAILY',
    columns: {
      event_date: 'DATETIME', label: 'STRING', traffic_class: 'STRING', frame_type: 'STRING',
      frames: 'INTEGER', attack_frames: 'INTEGER', frame_bytes: 'DECIMAL', distinct_bssids: 'INTEGER',
    },
  },
  topBssid: {
    view: 'vw_top_bssid', name: 'NexaSecure - Top BSSID', live: false, refresh: 'DAILY',
    columns: {
      bssid_pseudo: 'STRING', frames: 'INTEGER', attack_frames: 'INTEGER', attack_pct: 'DECIMAL',
      distinct_labels: 'INTEGER', first_seen: 'DATETIME', last_seen: 'DATETIME',
    },
  },
};

const DATASOURCE_ACTIONS = [
  'quicksight:DescribeDataSource', 'quicksight:DescribeDataSourcePermissions', 'quicksight:PassDataSource',
  'quicksight:UpdateDataSource', 'quicksight:DeleteDataSource', 'quicksight:UpdateDataSourcePermissions',
];
const DATASET_ACTIONS = [
  'quicksight:DescribeDataSet', 'quicksight:DescribeDataSetPermissions', 'quicksight:PassDataSet',
  'quicksight:DescribeIngestion', 'quicksight:ListIngestions', 'quicksight:UpdateDataSet',
  'quicksight:DeleteDataSet', 'quicksight:CreateIngestion', 'quicksight:CancelIngestion',
  'quicksight:UpdateDataSetPermissions',
];
const DASHBOARD_ACTIONS = [
  'quicksight:DescribeDashboard', 'quicksight:ListDashboardVersions', 'quicksight:UpdateDashboardPermissions',
  'quicksight:QueryDashboard', 'quicksight:UpdateDashboard', 'quicksight:DeleteDashboard',
  'quicksight:DescribeDashboardPermissions', 'quicksight:UpdateDashboardPublishedVersion',
];

// Los datasets en vivo llevan otro ID para que CloudFormation los cree nuevos (y borre los
// anteriores en SPICE) en vez de cambiar el modo de importación de un dataset existente.
const logicalId = (key) => `DataSet${key[0].toUpperCase()}${key.slice(1)}${DATASETS[key].live ? 'Live' : ''}`;
const dataSetId = (key) => `nexasecure-${key.toLowerCase()}${DATASETS[key].live ? '-live' : ''}-${STAGE}`;

// ---------------------------------------------------------------------------
// Ayudantes para la definición del dashboard
// ---------------------------------------------------------------------------
const col = (ds, name) => ({ DataSetIdentifier: ds, ColumnName: name });
const title = (text) => ({ Visibility: 'VISIBLE', FormatText: { PlainText: text } });
const dim = (id, ds, name) => ({ CategoricalDimensionField: { FieldId: id, Column: col(ds, name) } });
const dateDim = (id, ds, name, granularity) => ({
  DateDimensionField: { FieldId: id, Column: col(ds, name), DateGranularity: granularity },
});
const measure = (id, ds, name, aggregation) => ({
  NumericalMeasureField: { FieldId: id, Column: col(ds, name), AggregationFunction: { SimpleNumericalAggregation: aggregation } },
});
const percentile = (id, ds, name, value) => ({
  NumericalMeasureField: { FieldId: id, Column: col(ds, name), AggregationFunction: { PercentileAggregation: { PercentileValue: value } } },
});
const countOf = (id, ds, name) => ({
  CategoricalMeasureField: { FieldId: id, Column: col(ds, name), AggregationFunction: 'COUNT' },
});
const sortDesc = (fieldId) => [{ FieldSort: { FieldId: fieldId, Direction: 'DESC' } }];

const kpi = (id, text, value) => ({
  KPIVisual: { VisualId: id, Title: title(text), ChartConfiguration: { FieldWells: { Values: [value] } } },
});
const line = (id, text, category, value, color) => ({
  LineChartVisual: {
    VisualId: id, Title: title(text),
    ChartConfiguration: { FieldWells: { LineChartAggregatedFieldWells: { Category: [category], Values: [value], Colors: [color] } } },
  },
});
const bar = (id, text, { category, value, color, orientation, arrangement }) => ({
  BarChartVisual: {
    VisualId: id, Title: title(text),
    ChartConfiguration: {
      FieldWells: { BarChartAggregatedFieldWells: Object.assign(
        { Category: [category], Values: [value] }, color ? { Colors: [color] } : {}) },
      Orientation: orientation,
      BarsArrangement: arrangement,
      ...(orientation === 'HORIZONTAL'
        ? { SortConfiguration: { CategorySort: sortDesc(Object.values(value)[0].FieldId) } }
        : {}),
    },
  },
});
const pie = (id, text, category, value, donut) => ({
  PieChartVisual: {
    VisualId: id, Title: title(text),
    ChartConfiguration: Object.assign(
      { FieldWells: { PieChartAggregatedFieldWells: { Category: [category], Values: [value] } } },
      donut ? { DonutOptions: { ArcOptions: { ArcThickness: 'MEDIUM' } } } : {}),
  },
});
// QuickSight solo acepta PageSize de 100, 500, 1000 o 10000 filas.
const TABLE_PAGE_SIZE = 100;
const table = (id, text, ds, columns, sortColumn) => ({
  TableVisual: {
    VisualId: id, Title: title(text),
    ChartConfiguration: {
      FieldWells: { TableUnaggregatedFieldWells: {
        Values: columns.map((name) => ({ FieldId: `${id}-${name}`, Column: col(ds, name) })),
      } },
      SortConfiguration: {
        RowSort: sortDesc(`${id}-${sortColumn}`),
        PaginationConfiguration: { PageSize: TABLE_PAGE_SIZE, PageNumber: 1 },
      },
    },
  },
});

function dashboardDefinition() {
  const A = 'alerts';
  const H = 'hourly';
  const T = 'traffic';
  const B = 'topBssid';

  // [visual, columnIndex, columnSpan, rowIndex, rowSpan] sobre una grilla de 36 columnas
  const layout = [
    [kpi('kpi-alerts', 'Alertas totales', countOf('f-kpi-alerts', A, 'alert_id')), 0, 6, 0, 4],
    [kpi('kpi-high', 'Alertas altas o críticas', measure('f-kpi-high', A, 'is_high_or_critical', 'SUM')), 6, 6, 0, 4],
    [kpi('kpi-p95', 'Latencia de detección p95 (ms)', percentile('f-kpi-p95', A, 'detection_latency_ms', 95)), 12, 6, 0, 4],
    [kpi('kpi-sla', '% de alertas dentro del SLA (< 2 s)', measure('f-kpi-sla', A, 'sla_ok_pct', 'AVERAGE')), 18, 6, 0, 4],
    [kpi('kpi-frames', 'Tramas analizadas (batch)', measure('f-kpi-frames', T, 'frames', 'SUM')), 24, 6, 0, 4],
    [kpi('kpi-attack', 'Tramas de ataque', measure('f-kpi-attack', T, 'attack_frames', 'SUM')), 30, 6, 0, 4],

    [line('line-alerts', 'Alertas por hora y regla de detección',
      dateDim('f-line-hour', H, 'detected_hour', 'HOUR'), measure('f-line-alerts', H, 'alerts', 'SUM'),
      dim('f-line-rule', H, 'rule_type')), 0, 24, 4, 8],
    [pie('donut-severity', 'Alertas por severidad',
      dim('f-donut-sev', A, 'severity'), countOf('f-donut-count', A, 'alert_id'), true), 24, 12, 4, 8],

    [bar('bar-label', 'Tramas por categoría de AWID3', {
      category: dim('f-bar-label', T, 'label'), value: measure('f-bar-frames', T, 'frames', 'SUM'),
      orientation: 'HORIZONTAL', arrangement: 'CLUSTERED' }), 0, 18, 12, 8],
    [bar('bar-daily', 'Tráfico diario: normal frente a ataque', {
      category: dateDim('f-daily-date', T, 'event_date', 'DAY'), value: measure('f-daily-frames', T, 'frames', 'SUM'),
      color: dim('f-daily-class', T, 'traffic_class'), orientation: 'VERTICAL', arrangement: 'STACKED' }), 18, 18, 12, 8],

    [pie('pie-frametype', 'Tramas por tipo 802.11',
      dim('f-pie-type', T, 'frame_type'), measure('f-pie-frames', T, 'frames', 'SUM'), false), 0, 12, 20, 8],
    [table('table-bssid', 'Puntos de acceso más atacados (seudonimizados, k ≥ 5)', B,
      ['bssid_pseudo', 'frames', 'attack_frames', 'attack_pct', 'distinct_labels', 'last_seen'], 'attack_frames'), 12, 24, 20, 8],

    [table('table-alerts', 'Últimas alertas', A,
      ['detected_at', 'rule_type', 'severity', 'bssid_pseudo', 'observed_count', 'threshold', 'detection_latency_ms', 'sla_status'],
      'detected_at'), 0, 36, 28, 10],
  ];

  return {
    DataSetIdentifierDeclarations: Object.keys(DATASETS).map((key) => ({
      Identifier: key, DataSetArn: { 'Fn::GetAtt': [logicalId(key), 'Arn'] },
    })),
    Sheets: [{
      SheetId: 'soc-overview',
      Name: 'Centro de operaciones (SOC)',
      Visuals: layout.map(([visual]) => visual),
      Layouts: [{ Configuration: { GridLayout: { Elements: layout.map(([visual, c, cs, r, rs]) => ({
        ElementId: Object.values(visual)[0].VisualId, ElementType: 'VISUAL',
        ColumnIndex: c, ColumnSpan: cs, RowIndex: r, RowSpan: rs,
      })) } } }],
    }],
  };
}

module.exports = () => {
  const userArn = readUserArn();
  if (!/^arn:aws:quicksight:[a-z0-9-]+:\d{12}:user\/.+/.test(userArn)) {
    throw new Error('Define QUICKSIGHT_USER_ARN (arn:aws:quicksight:us-east-1:<cuenta>:user/default/<usuario>) '
      + 'en el entorno o en .envs/sls.env. Ver infrastructure/serverless/quicksight/README.md');
  }
  const owner = (actions) => [{ Principal: userArn, Actions: actions }];
  const trustedArn = `arn:aws:s3:::${TRUSTED}`;

  const Resources = {
    QuickSightAthenaRole: {
      Type: 'AWS::IAM::Role',
      Properties: {
        RoleName: `nexasecure-quicksight-athena-${STAGE}`,
        Description: 'Rol que QuickSight usa para consultar las vistas Serving en Athena',
        AssumeRolePolicyDocument: {
          Version: '2012-10-17',
          Statement: [{ Effect: 'Allow', Principal: { Service: 'quicksight.amazonaws.com' }, Action: 'sts:AssumeRole' }],
        },
        Policies: [{
          PolicyName: 'nexasecure-quicksight-serving-read',
          PolicyDocument: {
            Version: '2012-10-17',
            Statement: [
              {
                Sid: 'AthenaConsumptionWorkgroup',
                Effect: 'Allow',
                Action: ['athena:StartQueryExecution', 'athena:GetQueryExecution', 'athena:GetQueryResults',
                  'athena:StopQueryExecution', 'athena:GetWorkGroup', 'athena:GetQueryResultsStream'],
                Resource: `arn:aws:athena:${REGION}:${ACCOUNT}:workgroup/${WORKGROUP}`,
              },
              {
                Sid: 'AthenaCatalogMetadata',
                Effect: 'Allow',
                Action: ['athena:GetDataCatalog', 'athena:ListDataCatalogs', 'athena:ListWorkGroups',
                  'athena:GetDatabase', 'athena:ListDatabases', 'athena:GetTableMetadata', 'athena:ListTableMetadata'],
                Resource: '*',
              },
              {
                Sid: 'GlueCatalogRead',
                Effect: 'Allow',
                Action: ['glue:GetDatabase', 'glue:GetDatabases', 'glue:GetTable', 'glue:GetTables',
                  'glue:GetPartition', 'glue:GetPartitions', 'glue:BatchGetPartition'],
                Resource: [
                  `arn:aws:glue:${REGION}:${ACCOUNT}:catalog`,
                  `arn:aws:glue:${REGION}:${ACCOUNT}:database/${DATABASE}`,
                  `arn:aws:glue:${REGION}:${ACCOUNT}:table/${DATABASE}/*`,
                ],
              },
              {
                Sid: 'TrustedBucketOnly',
                Effect: 'Allow',
                Action: ['s3:GetBucketLocation', 's3:ListBucket', 's3:ListBucketMultipartUploads'],
                Resource: trustedArn,
              },
              {
                Sid: 'ReadTrustedObjects',
                Effect: 'Allow',
                Action: ['s3:GetObject'],
                Resource: `${trustedArn}/*`,
              },
              {
                Sid: 'WriteAthenaResults',
                Effect: 'Allow',
                Action: ['s3:PutObject', 's3:AbortMultipartUpload', 's3:ListMultipartUploadParts'],
                Resource: `${trustedArn}/athena-results/consumption/*`,
              },
            ],
          },
        }],
      },
    },

    AthenaDataSource: {
      Type: 'AWS::QuickSight::DataSource',
      Properties: {
        AwsAccountId: ACCOUNT,
        DataSourceId: `nexasecure-athena-${STAGE}`,
        Name: 'NexaSecure - Athena (Serving)',
        Type: 'ATHENA',
        DataSourceParameters: {
          AthenaParameters: { WorkGroup: WORKGROUP, RoleArn: { 'Fn::GetAtt': ['QuickSightAthenaRole', 'Arn'] } },
        },
        SslProperties: { DisableSsl: false },
        Permissions: owner(DATASOURCE_ACTIONS),
      },
    },
  };

  for (const [key, ds] of Object.entries(DATASETS)) {
    Resources[logicalId(key)] = {
      Type: 'AWS::QuickSight::DataSet',
      Properties: {
        AwsAccountId: ACCOUNT,
        DataSetId: dataSetId(key),
        Name: ds.name,
        ImportMode: ds.live ? 'DIRECT_QUERY' : 'SPICE',
        PhysicalTableMap: {
          [ds.view.replace(/_/g, '-')]: {
            RelationalTable: {
              DataSourceArn: { 'Fn::GetAtt': ['AthenaDataSource', 'Arn'] },
              Catalog: 'AwsDataCatalog',
              Schema: DATABASE,
              Name: ds.view,
              InputColumns: Object.entries(ds.columns).map(([Name, Type]) => ({ Name, Type })),
            },
          },
        },
        Permissions: owner(DATASET_ACTIONS),
      },
    };
    if (ds.live) continue; // consulta directa: no hay copia en SPICE que refrescar
    Resources[`${logicalId(key)}Refresh`] = {
      Type: 'AWS::QuickSight::RefreshSchedule',
      DependsOn: [logicalId(key)],
      Properties: {
        AwsAccountId: ACCOUNT,
        DataSetId: dataSetId(key),
        Schedule: {
          ScheduleId: `${dataSetId(key)}-refresh`,
          RefreshType: 'FULL_REFRESH',
          ScheduleFrequency: Object.assign(
            { Interval: ds.refresh, TimeZone: TIMEZONE },
            ds.refresh === 'DAILY' ? { TimeOfTheDay: '06:00' } : {}),
        },
      },
    };
  }

  Resources.SocDashboard = {
    Type: 'AWS::QuickSight::Dashboard',
    Properties: {
      AwsAccountId: ACCOUNT,
      DashboardId: `nexasecure-wids-soc-${STAGE}`,
      Name: 'NexaSecure WIDS - SOC',
      Definition: dashboardDefinition(),
      ValidationStrategy: { Mode: 'LENIENT' },
      DashboardPublishOptions: {
        AdHocFilteringOption: { AvailabilityStatus: 'ENABLED' },
        ExportToCSVOption: { AvailabilityStatus: 'ENABLED' },
        SheetControlsOption: { VisibilityState: 'EXPANDED' },
      },
      Permissions: owner(DASHBOARD_ACTIONS),
    },
  };

  Resources.DashboardUrlParameter = {
    Type: 'AWS::SSM::Parameter',
    Properties: {
      Name: `/nexasecure/${STAGE}/consumption/DashboardUrl`,
      Type: 'String',
      Value: `https://${REGION}.quicksight.aws.amazon.com/sn/dashboards/nexasecure-wids-soc-${STAGE}`,
      Description: 'URL del dashboard de QuickSight de NexaSecure',
    },
  };

  return {
    Resources,
    Outputs: {
      DashboardUrl: { Value: `https://${REGION}.quicksight.aws.amazon.com/sn/dashboards/nexasecure-wids-soc-${STAGE}` },
      DataSetIds: { Value: Object.keys(DATASETS).map(dataSetId).join(',') },
    },
  };
};
