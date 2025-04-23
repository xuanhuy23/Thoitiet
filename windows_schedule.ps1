# Script PowerShell để tạo tác vụ lập lịch cho hanoi_weather_cli.py trên Windows
# Chạy script này với quyền quản trị (Administrator) bằng cách nhấp chuột phải và chọn "Run as Administrator"
$ErrorActionPreference = "Stop"

# Lấy thư mục hiện tại của script
$ScriptDir = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
if (-not $ScriptDir) {
    $ScriptDir = Get-Location
}

# Đường dẫn đến Python và script
$PythonPath = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $PythonPath) {
    Write-Host "Không tìm thấy Python trong PATH. Vui lòng nhập đường dẫn đầy đủ đến Python:" -ForegroundColor Yellow
    $PythonPath = Read-Host
    if (-not (Test-Path $PythonPath)) {
        Write-Host "Đường dẫn không hợp lệ. Thoát script." -ForegroundColor Red
        exit 1
    }
}

$WeatherScriptPath = Join-Path -Path $ScriptDir -ChildPath "hanoi_weather_cli.py"
if (-not (Test-Path $WeatherScriptPath)) {
    Write-Host "Không tìm thấy file script hanoi_weather_cli.py trong thư mục hiện tại." -ForegroundColor Red
    Write-Host "Vui lòng chạy script này từ thư mục chứa file hanoi_weather_cli.py." -ForegroundColor Red
    exit 1
}

# Tên tác vụ
$TaskName = "HanoiWeatherHourly"
$TaskDescription = "Thu thập dữ liệu thời tiết Hà Nội mỗi giờ"

# Kiểm tra xem tác vụ đã tồn tại chưa
$TaskExists = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue

if ($TaskExists) {
    Write-Host "Tác vụ '$TaskName' đã tồn tại. Bạn có muốn ghi đè không? (Y/N)" -ForegroundColor Yellow
    $Overwrite = Read-Host
    if ($Overwrite -ne "Y" -and $Overwrite -ne "y") {
        Write-Host "Thoát script mà không thay đổi tác vụ hiện có." -ForegroundColor Green
        exit 0
    }
    
    # Xóa tác vụ hiện có
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Đã xóa tác vụ hiện có." -ForegroundColor Green
}

# Tạo action cho task
$Action = New-ScheduledTaskAction -Execute $PythonPath -Argument """$WeatherScriptPath""" -WorkingDirectory $ScriptDir

# Tạo trigger cho task (chạy mỗi giờ)
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 60) -RepetitionDuration ([TimeSpan]::MaxValue)

# Tạo cài đặt cho task
$Settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable -RunOnlyIfNetworkAvailable -WakeToRun

# Đăng ký task
try {
    $User = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    Register-ScheduledTask -TaskName $TaskName -Description $TaskDescription -Action $Action -Trigger $Trigger -Settings $Settings -User $User -RunLevel Highest -Force
    Write-Host "Đã tạo tác vụ lập lịch '$TaskName' thành công!" -ForegroundColor Green
    Write-Host "Script sẽ chạy mỗi giờ và thu thập dữ liệu thời tiết Hà Nội." -ForegroundColor Green
    
    # Kiểm tra môi trường
    Write-Host "`nThông tin tác vụ:" -ForegroundColor Cyan
    Write-Host "- Python: $PythonPath" -ForegroundColor Cyan
    Write-Host "- Script: $WeatherScriptPath" -ForegroundColor Cyan
    Write-Host "- Thư mục làm việc: $ScriptDir" -ForegroundColor Cyan
    Write-Host "- Người dùng: $User" -ForegroundColor Cyan
    
    # Kiểm tra API key
    $EnvFilePath = Join-Path -Path $ScriptDir -ChildPath ".env"
    if (Test-Path $EnvFilePath) {
        $EnvContent = Get-Content $EnvFilePath -ErrorAction SilentlyContinue
        $HasApiKey = $EnvContent -match "OPENWEATHERMAP_API_KEY=.+"
        if ($HasApiKey) {
            Write-Host "- API Key: Đã thiết lập trong file .env" -ForegroundColor Cyan
        } else {
            Write-Host "- API Key: CẢNH BÁO - Không tìm thấy trong file .env" -ForegroundColor Yellow
        }
    } else {
        Write-Host "- API Key: CẢNH BÁO - Không tìm thấy file .env" -ForegroundColor Yellow
        Write-Host "  Hãy chạy 'python hanoi_weather_cli.py --setup' để tạo file .env mẫu và cấu hình API key." -ForegroundColor Yellow
    }
    
    Write-Host "`nBạn có muốn chạy script ngay bây giờ để kiểm tra không? (Y/N)" -ForegroundColor Yellow
    $RunNow = Read-Host
    if ($RunNow -eq "Y" -or $RunNow -eq "y") {
        Write-Host "Đang chạy script..." -ForegroundColor Green
        Start-Process -FilePath $PythonPath -ArgumentList """$WeatherScriptPath""" -Wait -NoNewWindow
        Write-Host "Script đã chạy xong. Vui lòng kiểm tra file log để biết kết quả." -ForegroundColor Green
    }
} catch {
    Write-Host "Lỗi khi tạo tác vụ lập lịch: $_" -ForegroundColor Red
    exit 1
} 