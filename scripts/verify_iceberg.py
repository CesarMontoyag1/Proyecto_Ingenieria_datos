"""Verify actual Parquet bloom-filter offsets, reading only file footers from S3."""
import argparse
import io
import json
from concurrent.futures import ThreadPoolExecutor

from fastparquet import ParquetFile
from fastavro import reader

from aws_common import ROOT, aws_session, parameter
from iceberg_benchmark import metadata


def current_files(s3, manifest_list):
    def avro_records(path):
        bucket, key = path[5:].split('/', 1)
        body = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        return list(reader(io.BytesIO(body)))
    files = set()
    for manifest in avro_records(manifest_list):
        if manifest.get('content', 0) != 0:
            continue
        for entry in avro_records(manifest['manifest_path']):
            data_file = entry['data_file']
            if entry['status'] in (0, 1) and data_file.get('content', 0) == 0:
                files.add(data_file['file_path'])
    return sorted(files)


def inspect_footer(s3, path):
    bucket, key = path[5:].split('/', 1)
    trailer = s3.get_object(Bucket=bucket, Key=key, Range='bytes=-8')['Body'].read()
    if trailer[4:] != b'PAR1':
        raise ValueError('Not a Parquet file')
    length = int.from_bytes(trailer[:4], 'little')
    body = s3.get_object(Bucket=bucket, Key=key, Range=f'bytes=-{length + 8}')['Body'].read()
    parquet = ParquetFile(io.BytesIO(b'PAR1' + body))
    offsets = []
    for group in parquet.row_groups:
        for column in group.columns:
            if column.meta_data.path_in_schema in (['wlan.bssid'], ['wlan_x2Ebssid']):
                offsets.append(getattr(column.meta_data, 'bloom_filter_offset', None))
    return {'file': path, 'bssid_row_groups': len(offsets),
            'bssid_bloom_row_groups': sum(offset is not None and offset > 0 for offset in offsets)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', default='dev')
    args = parser.parse_args()
    session = aws_session()
    database = parameter(session, args.stage, 'glue/GlueCatalogDatabaseName')
    meta = metadata(session, database)
    if meta['properties'].get('nexasecure.layout.version') != '3':
        raise RuntimeError('Complete the migration first')
    s3 = session.client('s3')
    paths = current_files(s3, meta['manifest_list'])
    with ThreadPoolExecutor(max_workers=4) as pool:
        files = list(pool.map(lambda path: inspect_footer(s3, path), paths))
    data = {'snapshot_id': meta['snapshot_id'], 'files': files,
            'bssid_row_groups': sum(f['bssid_row_groups'] for f in files),
            'bssid_bloom_row_groups': sum(f['bssid_bloom_row_groups'] for f in files)}
    output = ROOT / 'docs/optimizacion/iceberg/evidencia/parquet_verification.json'
    output.write_text(json.dumps(data, indent=2) + '\n', encoding='utf-8')
    if not data['bssid_row_groups'] or data['bssid_row_groups'] != data['bssid_bloom_row_groups']:
        raise RuntimeError('Some BSSID row groups have no physical bloom filter; inspect evidence.')
    print(f'Verified actual bloom filters in all {len(files)} files, '
          f'{data["bssid_row_groups"]} BSSID row groups. Only metadata read; no event rows downloaded.')


if __name__ == '__main__':
    main()
