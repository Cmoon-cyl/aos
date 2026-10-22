$ErrorActionPreference = 'Stop'
# Prefer this PowerShell runtime's modules when launched from another version.
$env:PSModulePath = (Join-Path $PSHOME 'Modules') + [IO.Path]::PathSeparator + $env:PSModulePath
$InstallerArgs = @($args)
if ($InstallerArgs.Count -gt 0 -and $InstallerArgs[0] -eq '--help') {
    Write-Output 'Install AOS: install.ps1 [--prefix PATH] [--agent codex|claude|trae] [--no-path] [--non-interactive] [--json]'
    return
}
$Version = if ($env:AOS_AGENT_VERSION) { $env:AOS_AGENT_VERSION } else { '0.1.2' }
if ($Version -notmatch '^\d+\.\d+\.\d+$') { throw 'Invalid AOS_AGENT_VERSION' }
$Temp = Join-Path ([IO.Path]::GetTempPath()) ('aos-install-' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($Temp) | Out-Null
try {
    $Asset = "aos-agent-$Version.zip"
    $Tag = "aos-agent-v$Version"
    $Zip = Join-Path $Temp $Asset
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    foreach ($Name in @($Asset, "$Asset.sha256")) {
        Invoke-WebRequest -UseBasicParsing -Uri "https://github.com/Cmoon-cyl/aos/releases/download/$Tag/$Name" -OutFile (Join-Path $Temp $Name)
    }
    $Expected = ((Get-Content "$Zip.sha256" -Raw).Trim() -split '\s+')[0]
    if ((Get-FileHash $Zip -Algorithm SHA256).Hash.ToLowerInvariant() -ne $Expected) { throw 'AOS download checksum mismatch' }
    Expand-Archive $Zip -DestinationPath (Join-Path $Temp 'unpacked')
    & (Join-Path $Temp 'unpacked\aos-agent\skills\aos\scripts\setup.ps1') @InstallerArgs
} finally { Remove-Item $Temp -Recurse -Force }
