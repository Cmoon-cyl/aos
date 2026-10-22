$ErrorActionPreference = 'Stop'
$InstallerArgs = @($args)
$ScriptDirectory = if ($PSScriptRoot) { $PSScriptRoot } else { throw 'Run the saved setup.ps1 file.' }
$Prefix = if ($env:AOS_HOME) { $env:AOS_HOME } else { Join-Path ([Environment]::GetFolderPath('UserProfile')) '.aos' }
for ($i = 0; $i -lt $InstallerArgs.Count; $i++) {
    if ($InstallerArgs[$i] -eq '--prefix') {
        if ($i + 1 -ge $InstallerArgs.Count) { throw 'Missing --prefix value' }
        $Prefix = $InstallerArgs[$i + 1]
    } elseif ($InstallerArgs[$i] -like '--prefix=*') {
        $Prefix = $InstallerArgs[$i].Substring(9)
    }
}
if (-not [Environment]::Is64BitOperatingSystem -or $env:PROCESSOR_ARCHITECTURE -notin @('AMD64', 'x86')) {
    throw 'This release requires x64 Windows.'
}
$Prefix = [IO.Path]::GetFullPath($Prefix)
[IO.Directory]::CreateDirectory($Prefix) | Out-Null
$env:AOS_HOME = $Prefix
$UvVersion = '0.13.5'
$UvDigest = 'a38b8d1902b7cfbc76b423ba1bb456f623f42064cdc72628b6075b89afd1019a'
$UvDirectory = Join-Path $Prefix "bootstrap\uv\$UvVersion"
[IO.Directory]::CreateDirectory($UvDirectory) | Out-Null
$Uv = Join-Path $UvDirectory 'uv.exe'
$Valid = (Test-Path $Uv) -and (Test-Path "$Uv.sha256")
if ($Valid) { $Valid = (Get-FileHash $Uv -Algorithm SHA256).Hash.ToLowerInvariant() -eq (Get-Content "$Uv.sha256" -Raw).Trim() }
if (-not $Valid) {
    $Temp = Join-Path $Prefix ('bootstrap\uv-download-' + [Guid]::NewGuid().ToString('N'))
    [IO.Directory]::CreateDirectory($Temp) | Out-Null
    try {
        $Zip = Join-Path $Temp 'uv.zip'
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -UseBasicParsing -Uri "https://github.com/astral-sh/uv/releases/download/$UvVersion/uv-x86_64-pc-windows-msvc.zip" -OutFile $Zip
        if ((Get-FileHash $Zip -Algorithm SHA256).Hash.ToLowerInvariant() -ne $UvDigest) { throw 'uv archive checksum mismatch' }
        Expand-Archive $Zip -DestinationPath (Join-Path $Temp 'unpacked')
        $Binaries = @(Get-ChildItem (Join-Path $Temp 'unpacked') -Recurse -Filter uv.exe)
        if ($Binaries.Count -ne 1) { throw 'Expected exactly one uv.exe' }
        $Hash = (Get-FileHash $Binaries[0].FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        if (-not (Test-Path $Uv)) { Move-Item $Binaries[0].FullName $Uv }
        if ((Get-FileHash $Uv -Algorithm SHA256).Hash.ToLowerInvariant() -ne $Hash) { throw 'Existing uv.exe does not match the verified archive' }
        [IO.File]::WriteAllText("$Uv.sha256", $Hash)
    } finally { Remove-Item $Temp -Recurse -Force }
}
$env:UV_PYTHON_INSTALL_DIR = Join-Path $Prefix 'bootstrap\python'
$env:UV_CACHE_DIR = Join-Path $Prefix 'cache\uv'
$env:UV_PYTHON_BIN_DIR = Join-Path $Prefix 'bootstrap\bin'
& $Uv python install --no-config --no-bin --no-registry 3.11.15
if ($LASTEXITCODE -ne 0) { throw 'Managed Python installation failed' }
$Python = (& $Uv python find --no-config --no-project --managed-python 3.11.15).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Managed Python was not found' }
& $Python -X utf8 -I (Join-Path $ScriptDirectory 'aos_manager.py') install --uv $Uv @InstallerArgs
if ($LASTEXITCODE -ne 0) { throw "AOS installation failed (exit $LASTEXITCODE)" }
