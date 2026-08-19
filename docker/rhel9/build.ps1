# Builds SimpleKafka1C.so for RHEL 9 inside a container and copies the
# artifact to out/rhel9 on the host.
#
#   pwsh docker/rhel9/build.ps1
#   pwsh docker/rhel9/build.ps1 -VcpkgRef 2025.06.13 -Output out/rhel9

[CmdletBinding()]
param(
    [string]$Output = "out/rhel9",
    [string]$BaseImage = "rockylinux/rockylinux:9",
    [string]$VcpkgRef = "master",
    [string]$BuildType = "Release",
    [string]$Triplet = "x64-linux"
)

$ErrorActionPreference = "Stop"
$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "../..")

Push-Location $repoRoot
try {
    docker build `
        -f docker/rhel9/Dockerfile `
        --target export `
        --output $Output `
        --build-arg BASE_IMAGE=$BaseImage `
        --build-arg VCPKG_REF=$VcpkgRef `
        --build-arg BUILD_TYPE=$BuildType `
        --build-arg TRIPLET=$Triplet `
        .
    if ($LASTEXITCODE -ne 0) { throw "docker build failed with exit code $LASTEXITCODE" }

    Write-Host ""
    Write-Host "Artifacts in $Output :"
    Get-ChildItem $Output | Format-Table Name, Length, LastWriteTime
}
finally {
    Pop-Location
}
