# Surface set-up for the home stock checker.
# Run ONCE in PowerShell opened with "Run as administrator".
# What it does, in order:
#   1. turns on Windows' built-in remote-login service (OpenSSH server)
#   2. lets exactly one key in: the one that lives on Gurps' Mac. Password logins are switched off.
#   3. only accepts connections from your own home network
#   4. stops the tablet sleeping while it is plugged in
# Nothing is downloaded except the OpenSSH component from Windows Update. Nothing is removed.
$ErrorActionPreference = "Stop"
$key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK/AoZMXebR5mm3dhabS9yw05JLDmdHm0N81P4ktf2nM claude-on-gurps-mac-to-surface"

if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Write-Host "Please close this window, right-click PowerShell and choose 'Run as administrator', then run the same line again." -ForegroundColor Yellow; return
}

Write-Host "1/4  Turning on Windows' built-in remote login (this can take a few minutes)..."
$cap = Get-WindowsCapability -Online | Where-Object Name -like 'OpenSSH.Server*'
if ($cap.State -ne 'Installed') { Add-WindowsCapability -Online -Name $cap.Name | Out-Null }
Start-Service sshd                      # first start writes the default config
Set-Service  sshd -StartupType Automatic

Write-Host "2/4  Allowing only the Mac's key; switching password logins off..."
New-Item -ItemType Directory -Force -Path "C:\ProgramData\ssh" | Out-Null
$admKeys = "C:\ProgramData\ssh\administrators_authorized_keys"
Set-Content -Path $admKeys -Value $key -Encoding ascii
icacls $admKeys /inheritance:r /grant "*S-1-5-32-544:F" /grant "*S-1-5-18:F" | Out-Null
$userSsh = Join-Path $env:USERPROFILE ".ssh"
New-Item -ItemType Directory -Force -Path $userSsh | Out-Null
Set-Content -Path (Join-Path $userSsh "authorized_keys") -Value $key -Encoding ascii
$cfgPath = "C:\ProgramData\ssh\sshd_config"
$cfg = Get-Content $cfgPath
$cfg = $cfg -replace '^\s*#?\s*PasswordAuthentication\s+.*$', 'PasswordAuthentication no'
$cfg = $cfg -replace '^\s*#?\s*PubkeyAuthentication\s+.*$',   'PubkeyAuthentication yes'
if (-not ($cfg -match '^PasswordAuthentication no')) { $cfg += 'PasswordAuthentication no' }
Set-Content -Path $cfgPath -Value $cfg -Encoding ascii
New-Item -Path "HKLM:\SOFTWARE\OpenSSH" -Force | Out-Null
New-ItemProperty -Path "HKLM:\SOFTWARE\OpenSSH" -Name DefaultShell -PropertyType String -Force `
    -Value "C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe" | Out-Null
Restart-Service sshd

Write-Host "3/4  Accepting connections from your home network only..."
$rule = Get-NetFirewallRule -Name 'OpenSSH-Server-In-TCP' -ErrorAction SilentlyContinue
if (-not $rule) {
    New-NetFirewallRule -Name 'OpenSSH-Server-In-TCP' -DisplayName 'OpenSSH Server (home network only)' `
        -Enabled True -Direction Inbound -Protocol TCP -Action Allow -LocalPort 22 -Profile Any -RemoteAddress LocalSubnet | Out-Null
} else {
    Set-NetFirewallRule -Name 'OpenSSH-Server-In-TCP' -Enabled True -Profile Any -RemoteAddress LocalSubnet
}

Write-Host "4/4  Stopping the tablet sleeping while plugged in..."
powercfg /change standby-timeout-ac 0
powercfg /change hibernate-timeout-ac 0
powercfg /change monitor-timeout-ac 5
powercfg /setacvalueindex SCHEME_CURRENT SUB_BUTTONS LIDACTION 0
powercfg /setactive SCHEME_CURRENT

$ips = (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.IPAddress -notlike '169.254.*' -and $_.IPAddress -ne '127.0.0.1' }).IPAddress -join ', '
$os  = (Get-CimInstance Win32_OperatingSystem)
Write-Host ""
Write-Host "DONE. Tell Claude these lines:" -ForegroundColor Green
Write-Host ("  user:    " + $env:USERNAME)
Write-Host ("  address: " + $ips)
Write-Host ("  windows: " + $os.Caption + " build " + $os.BuildNumber + ", " + $os.OSArchitecture + ", " + [math]::Round($os.TotalVisibleMemorySize/1MB,1) + " GB memory")
