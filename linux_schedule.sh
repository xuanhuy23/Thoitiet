#!/bin/bash
# Script bash để tạo tác vụ cron cho hanoi_weather.py trên Linux/macOS

set -e

# Lấy thư mục hiện tại của script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Đường dẫn đến Python và script
PYTHON_PATH=$(which python3 2>/dev/null || which python 2>/dev/null)
if [ -z "$PYTHON_PATH" ]; then
    echo -e "\e[33mKhông tìm thấy Python trong PATH. Vui lòng nhập đường dẫn đầy đủ đến Python:\e[0m"
    read PYTHON_PATH
    if [ ! -f "$PYTHON_PATH" ]; then
        echo -e "\e[31mĐường dẫn không hợp lệ. Thoát script.\e[0m"
        exit 1
    fi
fi

WEATHER_SCRIPT_PATH="$SCRIPT_DIR/hanoi_weather.py"
if [ ! -f "$WEATHER_SCRIPT_PATH" ]; then
    echo -e "\e[31mKhông tìm thấy file script hanoi_weather.py trong thư mục hiện tại.\e[0m"
    echo -e "\e[31mVui lòng chạy script này từ thư mục chứa file hanoi_weather.py.\e[0m"
    exit 1
fi

# Kiểm tra quyền thực thi của script
if [ ! -x "$WEATHER_SCRIPT_PATH" ]; then
    echo -e "\e[33mThêm quyền thực thi cho script...\e[0m"
    chmod +x "$WEATHER_SCRIPT_PATH"
fi

# Kiểm tra quyền thực thi của Python
if [ ! -x "$PYTHON_PATH" ]; then
    echo -e "\e[31mPython không có quyền thực thi. Vui lòng kiểm tra lại cài đặt Python.\e[0m"
    exit 1
fi

# Kiểm tra API key
ENV_FILE_PATH="$SCRIPT_DIR/.env"
if [ -f "$ENV_FILE_PATH" ]; then
    if grep -q "OPENWEATHERMAP_API_KEY=.*" "$ENV_FILE_PATH"; then
        echo -e "\e[36mAPI Key: Đã thiết lập trong file .env\e[0m"
    else
        echo -e "\e[33mAPI Key: CẢNH BÁO - Không tìm thấy trong file .env\e[0m"
    fi
else
    echo -e "\e[33mAPI Key: CẢNH BÁO - Không tìm thấy file .env\e[0m"
    echo -e "\e[33mHãy chạy '$PYTHON_PATH $WEATHER_SCRIPT_PATH --setup' để tạo file .env mẫu và cấu hình API key.\e[0m"
    
    echo -e "\e[33mBạn có muốn tạo file .env ngay bây giờ không? (y/n)\e[0m"
    read CREATE_ENV
    if [[ "$CREATE_ENV" == "y" || "$CREATE_ENV" == "Y" ]]; then
        "$PYTHON_PATH" "$WEATHER_SCRIPT_PATH" --setup
    fi
fi

# Tạo crontab entry
CRON_CMD="0 * * * * $PYTHON_PATH $WEATHER_SCRIPT_PATH > $SCRIPT_DIR/cron_weather.log 2>&1"

# Kiểm tra nếu entry đã tồn tại
EXISTING_CRON=$(crontab -l 2>/dev/null | grep -F "$WEATHER_SCRIPT_PATH" || echo "")

if [ -n "$EXISTING_CRON" ]; then
    echo -e "\e[33mĐã tìm thấy tác vụ cron hiện có cho script này:\e[0m"
    echo -e "\e[33m$EXISTING_CRON\e[0m"
    echo -e "\e[33mBạn có muốn thay thế nó không? (y/n)\e[0m"
    read REPLACE_CRON
    
    if [[ "$REPLACE_CRON" != "y" && "$REPLACE_CRON" != "Y" ]]; then
        echo -e "\e[32mGiữ nguyên tác vụ cron hiện có.\e[0m"
        exit 0
    fi
    
    # Xóa entry cũ
    crontab -l 2>/dev/null | grep -v "$WEATHER_SCRIPT_PATH" | crontab -
    echo -e "\e[32mĐã xóa tác vụ cron cũ.\e[0m"
fi

# Thêm entry mới
(crontab -l 2>/dev/null; echo "$CRON_CMD") | crontab -
echo -e "\e[32mĐã thêm tác vụ cron mới.\e[0m"

echo
echo -e "\e[36mThông tin tác vụ:\e[0m"
echo -e "\e[36m- Python: $PYTHON_PATH\e[0m"
echo -e "\e[36m- Script: $WEATHER_SCRIPT_PATH\e[0m"
echo -e "\e[36m- Thư mục làm việc: $SCRIPT_DIR\e[0m"
echo -e "\e[36m- Lịch trình: Chạy mỗi giờ (0 * * * *)\e[0m"
echo -e "\e[36m- Log file: $SCRIPT_DIR/cron_weather.log\e[0m"

echo
echo -e "\e[33mBạn có muốn chạy script ngay bây giờ để kiểm tra không? (y/n)\e[0m"
read RUN_NOW
if [[ "$RUN_NOW" == "y" || "$RUN_NOW" == "Y" ]]; then
    echo -e "\e[32mĐang chạy script...\e[0m"
    "$PYTHON_PATH" "$WEATHER_SCRIPT_PATH"
    echo -e "\e[32mScript đã chạy xong. Vui lòng kiểm tra file log để biết kết quả.\e[0m"
fi

echo
echo -e "\e[32mHoàn tất! Script sẽ tự động chạy mỗi giờ để thu thập dữ liệu thời tiết Hà Nội.\e[0m" 