[CmdletBinding()]
param(
    [ValidateSet('Create', 'Build', 'Deploy', 'Status', 'Forward')]
    [string]$Action = 'Status',
    [ValidateRange(1024, 65535)]
    [int]$LocalPort = 18080
)

$ErrorActionPreference = 'Stop'
$repoPath = Split-Path -Parent $PSScriptRoot
$projectsPath = Split-Path -Parent $repoPath
$resourcePath = Join-Path $projectsPath 'resources/pay_ops'
$runtimePath = Join-Path $resourcePath 'runtime'
$kubeconfigPath = Join-Path $runtimePath 'kubeconfig'
$kindPath = Join-Path $resourcePath 'tools/kind-v0.33.0.exe'
$manifestPath = Join-Path $repoPath 'infra/kubernetes/local'
$nodeImage = 'kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0'
$imageName = 'payops-sandbox:local'
$kubeArgs = @('--kubeconfig', $kubeconfigPath, '--context', 'kind-payops-dev')

if ($Action -ne 'Build' -and -not (Test-Path -LiteralPath $kindPath)) {
    throw "Pinned kind binary missing at $kindPath. See infra/kubernetes/local/README.md for setup."
}

function Invoke-CheckedTool {
    <# Native tools must fail the task instead of allowing a later command to hide failure. #>
    param([string]$Executable, [string[]]$ToolArguments)
    & $Executable @ToolArguments
    if ($LASTEXITCODE -ne 0) {
        throw "$Executable failed with exit code $LASTEXITCODE"
    }
}

function Assert-PayOpsCluster {
    <# A dedicated loopback kubeconfig prevents accidental writes to another environment. #>
    if (-not (Test-Path -LiteralPath $kubeconfigPath)) {
        throw 'PayOps kubeconfig is missing. Run the Create action first.'
    }
    $server = Invoke-CheckedTool 'kubectl' ($kubeArgs + @(
        'config', 'view', '--minify', '-o', 'jsonpath={.clusters[0].cluster.server}'
    ))
    if ($server -notmatch '^https://127\.0\.0\.1:\d+$') {
        throw 'The PayOps cluster API must bind to loopback.'
    }
    $clusterNames = @(Invoke-CheckedTool $kindPath @('get', 'clusters'))
    if ('payops-dev' -notin $clusterNames) {
        throw 'The dedicated payops-dev kind cluster does not exist.'
    }
}

switch ($Action) {
    'Create' {
        $clusters = @(Invoke-CheckedTool $kindPath @('get', 'clusters'))
        if ('payops-dev' -in $clusters) {
            Assert-PayOpsCluster
            Write-Output 'Existing payops-dev cluster verified; no replacement performed.'
            break
        }
        New-Item -ItemType Directory -Path $runtimePath -Force | Out-Null
        Invoke-CheckedTool $kindPath @(
            'create', 'cluster', '--name', 'payops-dev',
            '--config', (Join-Path $manifestPath 'kind.yaml'),
            '--kubeconfig', $kubeconfigPath, '--image', $nodeImage, '--wait', '120s'
        )
    }
    'Build' {
        Invoke-CheckedTool 'docker' @('build', '--tag', $imageName, $repoPath)
    }
    'Deploy' {
        Assert-PayOpsCluster
        Invoke-CheckedTool 'docker' @('image', 'inspect', '--format', '{{.Id}}', $imageName)
        Invoke-CheckedTool $kindPath @('load', 'docker-image', $imageName, '--name', 'payops-dev')
        Invoke-CheckedTool 'kubectl' ($kubeArgs + @('apply', '-k', $manifestPath))
        # A local tag is reused deliberately; restart picks up the newly loaded image.
        Invoke-CheckedTool 'kubectl' ($kubeArgs + @(
            '-n', 'payops-sandbox', 'rollout', 'restart', 'deployment',
            '-l', 'app.kubernetes.io/part-of=payops'
        ))
        Invoke-CheckedTool 'kubectl' ($kubeArgs + @(
            '-n', 'payops-sandbox', 'rollout', 'status', 'deployment',
            '-l', 'app.kubernetes.io/part-of=payops', '--timeout=120s'
        ))
    }
    'Status' {
        Assert-PayOpsCluster
        Invoke-CheckedTool 'kubectl' ($kubeArgs + @('get', 'nodes'))
        Invoke-CheckedTool 'kubectl' ($kubeArgs + @('-n', 'payops-sandbox', 'get', 'pods,services'))
    }
    'Forward' {
        Assert-PayOpsCluster
        Invoke-CheckedTool 'kubectl' ($kubeArgs + @(
            '-n', 'payops-sandbox', 'port-forward', 'service/payments-api',
            "${LocalPort}:8080", '--address', '127.0.0.1'
        ))
    }
}
