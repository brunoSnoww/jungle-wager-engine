# Compose owns the infrastructure; Tilt only adds the local iteration loop.
# No remote Tilt extensions; CLI equivalents remain in Makefile.
analytics_settings(False)
docker_prune_settings(disable=True)
docker_compose('docker-compose.yml', project_name='jungle-gaming')

docker_build(
    'jungle-api:local',
    context='.',
    dockerfile='Dockerfile',
    target='dev',
    ignore=['.omc', '.git', '.cache'],
    live_update=[
        fall_back_on(['go.mod', 'go.sum', 'Dockerfile', 'migrations']),
        sync('./internal', '/app/internal'),
        sync('./cmd', '/app/cmd'),
        run('cd /app && CGO_ENABLED=0 go build -trimpath -o /app/bin/api ./cmd/api', trigger=['internal', 'cmd']),
        restart_container(),
    ],
)

dc_resource('postgres', labels=['infrastructure'])
dc_resource('keycloak', labels=['infrastructure'])
dc_resource('localstack', labels=['infrastructure'])
dc_resource('localstack-init', labels=['infrastructure'], resource_deps=['localstack'])
dc_resource('migrate', labels=['tools'], resource_deps=['postgres'])
dc_resource('app-1', labels=['backend'], resource_deps=['migrate', 'keycloak', 'localstack-init'])

local_resource('db-migrate', 'make migrate', trigger_mode=TRIGGER_MODE_MANUAL, auto_init=False, labels=['tools'], resource_deps=['postgres'])
local_resource('test-unit', 'make test', trigger_mode=TRIGGER_MODE_MANUAL, auto_init=False, labels=['testing'])
local_resource('test-integration', 'make integration', trigger_mode=TRIGGER_MODE_MANUAL, auto_init=False, labels=['testing'], resource_deps=['app-1'])
local_resource('demo-concurrency', 'make concurrency', trigger_mode=TRIGGER_MODE_MANUAL, auto_init=False, labels=['testing'], resource_deps=['app-1'])
