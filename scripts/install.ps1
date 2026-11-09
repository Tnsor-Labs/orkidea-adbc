<#
.SYNOPSIS
Installs the Orkidea ADBC driver and registers it with the ADBC driver
manager, so it can be loaded as driver "orkidea".

.EXAMPLE
irm https://github.com/Tnsor-Labs/orkidea-adbc/releases/latest/download/install.ps1 | iex

.EXAMPLE
.\install.ps1 -Version v0.2.0

.EXAMPLE
.\install.ps1 -Archive .\orkidea-adbc-0.2.0-windows_amd64.zip

.NOTES
Downloads are verified against the release's SHA256SUMS. When cosign is
installed, SHA256SUMS is also verified against its Sigstore signature. The
driver is installed for the current user under %LOCALAPPDATA%.
#>
[CmdletBinding()]
param(
    [string]$Version = "latest",
    [string]$Archive = ""
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$Repo = "Tnsor-Labs/orkidea-adbc"

function Say([string]$Message) { Write-Host "install.ps1: $Message" }

$arch = switch ($env:PROCESSOR_ARCHITECTURE) {
    "AMD64" { "amd64" }
    "ARM64" { "arm64" }
    default { throw "unsupported architecture $($env:PROCESSOR_ARCHITECTURE)" }
}
$platform = "windows_$arch"

$work = Join-Path ([System.IO.Path]::GetTempPath()) ("orkidea-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $work | Out-Null
try {
    if ($Archive) {
        if (-not (Test-Path $Archive)) { throw "archive $Archive not found" }
        $asset = Split-Path $Archive -Leaf
        Copy-Item $Archive (Join-Path $work $asset)
        $sums = Join-Path (Split-Path (Resolve-Path $Archive) -Parent) "SHA256SUMS"
        if (Test-Path $sums) {
            Copy-Item $sums (Join-Path $work "SHA256SUMS")
        } else {
            Say "no SHA256SUMS next to $Archive; skipping checksum verification"
        }
    } else {
        if ($Version -eq "latest") {
            $release = Invoke-RestMethod "https://api.github.com/repos/$Repo/releases/latest"
            $Version = $release.tag_name
        }
        $number = $Version.TrimStart("v")
        $asset = "orkidea-adbc-$number-$platform.zip"
        $base = "https://github.com/$Repo/releases/download/v$number"
        Say "downloading $asset"
        Invoke-WebRequest "$base/$asset" -OutFile (Join-Path $work $asset)
        Invoke-WebRequest "$base/SHA256SUMS" -OutFile (Join-Path $work "SHA256SUMS")
        if (Get-Command cosign -ErrorAction SilentlyContinue) {
            $bundle = Join-Path $work "SHA256SUMS.sigstore.json"
            Invoke-WebRequest "$base/SHA256SUMS.sigstore.json" -OutFile $bundle
            & cosign verify-blob --bundle $bundle `
                --certificate-identity-regexp "^https://github.com/$Repo/\.github/workflows/release\.yml@refs/tags/v" `
                --certificate-oidc-issuer https://token.actions.githubusercontent.com `
                (Join-Path $work "SHA256SUMS") *> $null
            if ($LASTEXITCODE -ne 0) { throw "SHA256SUMS signature verification failed" }
            Say "verified the Sigstore signature of SHA256SUMS"
        }
    }

    $sumsFile = Join-Path $work "SHA256SUMS"
    if (Test-Path $sumsFile) {
        $line = Get-Content $sumsFile | Where-Object { ($_ -split "\s+\*?")[1] -eq $asset }
        if (-not $line) { throw "$asset is not listed in SHA256SUMS" }
        $expected = ($line -split "\s+")[0].ToLowerInvariant()
        $actual = (Get-FileHash (Join-Path $work $asset) -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actual -ne $expected) { throw "checksum mismatch for $asset" }
        Say "verified the SHA-256 checksum of $asset"
    }

    if ($asset -notmatch "^orkidea-adbc-(.+)-$([regex]::Escape($platform))\.zip$") {
        throw "$asset is not a $platform archive"
    }
    $number = $Matches[1]

    # The ADBC driver manager reads manifests from the LocalAppData known
    # folder, so use the same API rather than the environment variable.
    $localAppData = [Environment]::GetFolderPath([Environment+SpecialFolder]::LocalApplicationData)
    $installDir = Join-Path $localAppData "orkidea-adbc\$number"
    $manifestDir = Join-Path $localAppData "ADBC\Drivers"
    New-Item -ItemType Directory -Force -Path $manifestDir | Out-Null

    Expand-Archive (Join-Path $work $asset) -DestinationPath $work -Force
    if (Test-Path $installDir) { Remove-Item -Recurse -Force $installDir }
    New-Item -ItemType Directory -Force -Path (Split-Path $installDir -Parent) | Out-Null
    Move-Item (Join-Path $work "orkidea-adbc-$number-$platform") $installDir
    $libPath = Join-Path $installDir "lib\adbc_driver_orkidea.dll"
    if (-not (Test-Path $libPath)) { throw "the archive does not contain lib\adbc_driver_orkidea.dll" }
    if ($libPath.Contains("'")) { throw "install path must not contain a single quote: $libPath" }

    # TOML literal strings ('...') take the Windows path verbatim.
    $manifest = @"
manifest_version = 1
name = "orkidea"
version = "$number"

[Driver]
entrypoint = "OrkideaDriverInit"

[Driver.shared]
$platform = '$libPath'
"@
    $manifestPath = Join-Path $manifestDir "orkidea.toml"
    [System.IO.File]::WriteAllText($manifestPath, $manifest, (New-Object System.Text.UTF8Encoding $false))

    Say "installed Orkidea ADBC $number to $installDir"
    Say "registered driver `"orkidea`" in $manifestPath"
} finally {
    Remove-Item -Recurse -Force $work -ErrorAction SilentlyContinue
}
