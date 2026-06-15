#!/usr/bin/env python3
"""
package.py — builds fraud-detection-demo-packaged.yaml and source.zip.

Usage:
    python cloudformation/package.py

After deploying the CFN stack:
    BUCKET=$(aws cloudformation describe-stacks --stack-name fraud-demo \\
      --query "Stacks[0].Outputs[?OutputKey=='BootstrapBucketName'].OutputValue" \\
      --output text)
    aws s3 cp cloudformation/source.zip s3://$BUCKET/buildspec/source.zip
    aws codebuild start-build --project-name fraud-demo-bootstrap
"""
import io, os, sys, zipfile
from collections import Counter

SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT    = os.path.dirname(SCRIPT_DIR)
TEMPLATE_IN  = os.path.join(SCRIPT_DIR, 'fraud-detection-demo.yaml')
TEMPLATE_OUT = os.path.join(SCRIPT_DIR, 'fraud-detection-demo-packaged.yaml')
SOURCE_ZIP   = os.path.join(SCRIPT_DIR, 'source.zip')


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _read(rel_path):
    full = os.path.join(REPO_ROOT, rel_path)
    if not os.path.exists(full):
        print('  WARNING: ' + rel_path + ' not found')
        return None
    with open(full, 'r', encoding='utf-8') as f:
        return f.read()


# ---------------------------------------------------------------------------
# Bootstrap scripts — each function returns a plain string (no nesting)
# ---------------------------------------------------------------------------

def buildspec_content():
    lines = [
        'version: 0.2',
        'phases:',
        '  install:',
        '    runtime-versions:',
        '      python: 3.11',
        '    commands:',
        '      - pip install --upgrade pip --quiet',
        '      - pip install --quiet boto3 psycopg2-binary',
        '  pre_build:',
        '    commands:',
        '      - echo Logging in to ECR',
        '      - aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com',
        '  build:',
        '    commands:',
        '      - echo Step 1 - Build and push Docker image',
        '      - docker build -t $ECR_REPO_URI:latest ./data_generator/',
        '      - docker push $ECR_REPO_URI:latest',
        '      - echo Step 2 - Launch ECS data generator',
        '      - python start_datagen.py || echo WARNING ECS launch failed - start manually if needed',
        '  post_build:',
        '    commands:',
        '      - echo Bootstrap complete',
    ]
    return '\n'.join(lines) + '\n'


def start_datagen_content():
    lines = [
        'import boto3, os, sys',
        '',
        'region   = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")',
        'cluster  = os.environ.get("ECS_CLUSTER", "fraud-demo-datagen")',
        'task_def = os.environ.get("ECS_TASK_DEF", "fraud-demo-datagen")',
        'subnet   = os.environ.get("ECS_SUBNET", "")',
        'sg       = os.environ.get("ECS_SG", "")',
        '',
        'if not subnet or not sg:',
        '    print("ECS_SUBNET or ECS_SG not set - skipping ECS launch")',
        '    sys.exit(0)',
        '',
        'ecs   = boto3.client("ecs", region_name=region)',
        'tasks = ecs.list_tasks(cluster=cluster).get("taskArns", [])',
        'for t in tasks:',
        '    ecs.stop_task(cluster=cluster, task=t, reason="Bootstrap restart")',
        '    print("Stopped existing task " + t)',
        '',
        'resp = ecs.run_task(',
        '    cluster=cluster, taskDefinition=task_def, launchType="FARGATE", count=1,',
        '    networkConfiguration={"awsvpcConfiguration": {',
        '        "subnets": [subnet], "securityGroups": [sg], "assignPublicIp": "ENABLED"}})',
        '',
        'if resp.get("tasks"):',
        '    print("Started ECS task: " + resp["tasks"][0]["taskArn"])',
        'elif resp.get("failures"):',
        '    print("ECS task failed: " + str(resp["failures"]))',
        '    sys.exit(1)',
    ]
    return '\n'.join(lines) + '\n'


def kinesis_producer_content():
    lines = [
        '"""',
        'kinesis_producer.py — replaces kafka_producer.py.',
        'Uses boto3 kinesis:PutRecord with standard IAM auth.',
        '"""',
        'import json, os, boto3',
        '',
        '_client = None',
        '',
        'def _get_client():',
        '    global _client',
        '    if _client is None:',
        '        _client = boto3.client("kinesis", region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"))',
        '    return _client',
        '',
        'def build_producer(**kwargs):',
        '    class KinesisProducer:',
        '        def send(self, stream, key=None, value=None):',
        '            data = json.dumps(value).encode() if not isinstance(value, bytes) else value',
        '            _get_client().put_record(StreamName=stream, Data=data, PartitionKey=key or "default")',
        '            return self',
        '        def add_errback(self, fn): return self',
        '        def flush(self): pass',
        '        def close(self): pass',
        '    return KinesisProducer()',
        '',
        'def send_event(producer, stream, event, key=None, on_error=None):',
        '    try:',
        '        producer.send(stream, key=key or "default", value=event)',
        '    except Exception as e:',
        '        if on_error:',
        '            on_error(e)',
        '        else:',
        '            print("Kinesis send error: " + str(e))',
    ]
    return '\n'.join(lines) + '\n'


def dockerfile_content():
    lines = [
        'FROM python:3.11-slim',
        'WORKDIR /app',
        'RUN apt-get update && apt-get install -y --no-install-recommends libpq-dev gcc && rm -rf /var/lib/apt/lists/*',
        'COPY requirements.txt .',
        'RUN pip install --no-cache-dir -r requirements.txt',
        'COPY . .',
        'ENV PYTHONUNBUFFERED=1',
        'ENTRYPOINT ["python", "main.py"]',
    ]
    return '\n'.join(lines) + '\n'


def requirements_content():
    lines = [
        'boto3==1.34.69',
        'psycopg2-binary==2.9.9',
        'faker==24.3.0',
        'numpy==1.26.4',
        'scipy==1.12.0',
        'python-dateutil==2.9.0',
        'pytz==2024.1',
        'requests==2.31.0',
        'pydantic==2.6.4',
    ]
    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------
# Patch helpers — read source files and apply Kinesis substitutions
# ---------------------------------------------------------------------------

def _patch_config(original):
    """Replace Kafka topic/config references with Kinesis env-var equivalents."""
    replacements = [
        ('TOPIC_TRADES: str = "trades.raw"',
         'TOPIC_TRADES: str = os.getenv("STREAM_TRADES", "fraud-demo-trades")'),
        ('TOPIC_SESSIONS: str = "sessions.raw"',
         'TOPIC_SESSIONS: str = os.getenv("STREAM_SESSIONS", "fraud-demo-sessions")'),
        ('TOPIC_REGISTRATIONS: str = "registrations.raw"',
         'TOPIC_REGISTRATIONS: str = os.getenv("STREAM_REGISTRATIONS", "fraud-demo-registrations")'),
        ('TOPIC_API_CALLS: str = "api.calls"',
         'TOPIC_API_CALLS: str = os.getenv("STREAM_API_CALLS", "fraud-demo-api-calls")'),
        ('MSK_CLUSTER_ARN: str = os.getenv("MSK_CLUSTER_ARN", "")',
         '# Kinesis does not need cluster ARN'),
        ('MSK_BOOTSTRAP_SERVERS: str = os.getenv("MSK_BOOTSTRAP_SERVERS", "localhost:9092")',
         '# Kinesis does not need bootstrap servers'),
        ('USE_IAM_AUTH: bool = os.getenv("USE_IAM_AUTH", "true").lower() == "true"',
         'USE_KINESIS: bool = os.getenv("USE_KINESIS", "true").lower() == "true"'),
    ]
    result = original
    for old, new in replacements:
        result = result.replace(old, new)
    return result


def _patch_main(original):
    """Replace kafka_producer import and build_producer call."""
    result = original.replace(
        'from producers.kafka_producer import build_producer, send_event',
        'from producers.kinesis_producer import build_producer, send_event',
    )
    old_build = (
        'producer = build_producer(\n'
        '        bootstrap_servers=Config.MSK_BOOTSTRAP_SERVERS,\n'
        '        use_iam_auth=Config.USE_IAM_AUTH,\n'
        '        region=Config.AWS_REGION,\n'
        '        msk_cluster_arn=Config.MSK_CLUSTER_ARN,\n'
        '    )'
    )
    new_build = 'producer = build_producer(region=Config.AWS_REGION)'
    result = result.replace(old_build, new_build)
    return result


# ---------------------------------------------------------------------------
# Source files from the repo to include in the zip
# ---------------------------------------------------------------------------

SOURCE_FILES = {
    'data_generator/personas/base.py':               'data_generator/personas/base.py',
    'data_generator/personas/normal_trader.py':      'data_generator/personas/normal_trader.py',
    'data_generator/personas/coordinated_ring.py':   'data_generator/personas/coordinated_ring.py',
    'data_generator/personas/ato_attacker.py':       'data_generator/personas/ato_attacker.py',
    'data_generator/personas/abusive_registrant.py': 'data_generator/personas/abusive_registrant.py',
    'data_generator/personas/system_abuser.py':      'data_generator/personas/system_abuser.py',
    'data_generator/utils/geo_data.py':              'data_generator/utils/geo_data.py',
    'data_generator/utils/device_fingerprint.py':    'data_generator/utils/device_fingerprint.py',
    'data_generator/utils/kyc_generator.py':         'data_generator/utils/kyc_generator.py',
    'data_generator/seeders/rds_seeder.py':          'data_generator/seeders/rds_seeder.py',
    'data_generator/seeders/s3_seeder.py':           'data_generator/seeders/s3_seeder.py',
}

INIT_FILES = [
    'data_generator/__init__.py',
    'data_generator/personas/__init__.py',
    'data_generator/producers/__init__.py',
    'data_generator/seeders/__init__.py',
    'data_generator/utils/__init__.py',
]


# ---------------------------------------------------------------------------
# Build source.zip
# ---------------------------------------------------------------------------

def build_source_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:

        # Bootstrap scripts
        zf.writestr('buildspec.yml',    buildspec_content())
        zf.writestr('start_datagen.py', start_datagen_content())

        # __init__ stubs
        for path in INIT_FILES:
            zf.writestr(path, '')

        # Generated/patched files
        zf.writestr('data_generator/Dockerfile',       dockerfile_content())
        zf.writestr('data_generator/requirements.txt', requirements_content())
        zf.writestr('data_generator/producers/kinesis_producer.py', kinesis_producer_content())

        # Patched config.py
        raw_config = _read('data_generator/config.py')
        if raw_config:
            zf.writestr('data_generator/config.py', _patch_config(raw_config))
            print('  Added data_generator/config.py (patched)')

        # Patched main.py
        raw_main = _read('data_generator/main.py')
        if raw_main:
            zf.writestr('data_generator/main.py', _patch_main(raw_main))
            print('  Added data_generator/main.py (patched)')

        # Repo source files
        for src_rel, dst_path in SOURCE_FILES.items():
            content = _read(src_rel)
            if content:
                zf.writestr(dst_path, content)
                print('  Added ' + dst_path)

    buf.seek(0)
    data = buf.read()
    with open(SOURCE_ZIP, 'wb') as f:
        f.write(data)

    size_kb = len(data) / 1024
    print('\nSource zip: ' + SOURCE_ZIP + ' (' + str(round(size_kb, 1)) + ' KB)')
    with zipfile.ZipFile(SOURCE_ZIP) as z:
        names = z.namelist()
        print('  Contains ' + str(len(names)) + ' files:')
        for n in sorted(names):
            info = z.getinfo(n)
            print('    ' + n + ' (' + str(info.file_size) + ' bytes)')

    return data


# ---------------------------------------------------------------------------
# Process template (copy + validate)
# ---------------------------------------------------------------------------

def process_template():
    with open(TEMPLATE_IN, 'r', encoding='utf-8') as f:
        content = f.read()
    with open(TEMPLATE_OUT, 'w', encoding='utf-8') as f:
        f.write(content)

    size_kb = os.path.getsize(TEMPLATE_OUT) / 1024
    print('\nTemplate: ' + TEMPLATE_OUT)
    print('  Size: ' + str(round(size_kb, 1)) + ' KB')

    # Duplicate resource key check
    with open(TEMPLATE_OUT, encoding='utf-8') as f:
        lines = f.readlines()
    keys, in_res = [], False
    for line in lines:
        stripped = line.rstrip()
        if stripped == 'Resources:':
            in_res = True
            continue
        if in_res and stripped in ('Outputs:', 'Conditions:', 'Mappings:'):
            break
        if (in_res and len(line) > 3
                and line[0] == ' ' and line[1] == ' '
                and line[2] != ' ' and line[2] != '#'
                and ':' in line):
            keys.append(line.strip().split(':')[0].strip())
    dups = {k: v for k, v in Counter(keys).items() if v > 1}
    if dups:
        print('  ERROR: Duplicate resource keys: ' + str(dups))
        sys.exit(1)
    else:
        print('  Duplicate key check: OK (' + str(len(keys)) + ' resources)')

    # YAML parse check
    try:
        import yaml as _yaml
        for tag in ['!Ref', '!Sub', '!GetAtt', '!Select', '!GetAZs', '!If', '!Not',
                    '!And', '!Or', '!Equals', '!Join', '!Split', '!FindInMap',
                    '!ImportValue', '!Condition', '!Base64', '!Cidr']:
            _yaml.add_multi_constructor(tag, lambda l, t, n: None, Loader=_yaml.SafeLoader)
        with open(TEMPLATE_OUT, encoding='utf-8') as f:
            _yaml.safe_load(f.read())
        print('  YAML parse check: OK')
    except ImportError:
        print('  YAML parse check: skipped (pyyaml not installed)')
    except Exception as e:
        print('  YAML ERROR: ' + str(e))
        sys.exit(1)

    if size_kb > 460:
        print('  NOTE: Upload via S3 URL (>' + str(460) + ' KB)')
    else:
        print('  Within console upload limit')

    return size_kb


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print('=== Building template ===')
    size_kb = process_template()

    print('\n=== Building source.zip ===')
    build_source_zip()

    print("""
=== Deploy ===

Step 1 — Deploy CloudFormation (one time):
  aws cloudformation deploy \\
    --template-file cloudformation/fraud-detection-demo-packaged.yaml \\
    --stack-name fraud-demo \\
    --capabilities CAPABILITY_NAMED_IAM \\
    --parameter-overrides AlertEmail=<email> RdsMasterPassword=<password>

Step 2 — Upload source.zip and trigger bootstrap (after CREATE_COMPLETE):
  BUCKET=$(aws cloudformation describe-stacks --stack-name fraud-demo \\
    --query "Stacks[0].Outputs[?OutputKey=='BootstrapBucketName'].OutputValue" \\
    --output text)
  aws s3 cp cloudformation/source.zip s3://$BUCKET/buildspec/source.zip
  aws codebuild start-build --project-name fraud-demo-bootstrap
""")


if __name__ == '__main__':
    main()
