#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command line interface cho ứng dụng Thời tiết Hà Nội

Script này cung cấp giao diện dòng lệnh để thu thập dữ liệu thời tiết Hà Nội
và lưu trữ dữ liệu đó vào cơ sở dữ liệu SQLite.
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Import module thời tiết
import weather_module

# --- Xử lý Đường dẫn Tệp và Thư mục ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)  # Chuyển thư mục làm việc đến thư mục chứa script

# --- Cấu hình mặc định ---
LOG_FILENAME = os.path.join(SCRIPT_DIR, 'hanoi_weather.log')
DB_PATH = os.path.join(SCRIPT_DIR, 'hanoi_weather.db')
ENV_FILE = os.path.join(SCRIPT_DIR, '.env')
CITY = "Hanoi"

# --- Tạo file .env mẫu nếu không tồn tại ---
def create_sample_env_file(env_file):
    """Tạo file .env mẫu nếu chưa tồn tại."""
    logger = logging.getLogger(__name__)
    
    if not os.path.exists(env_file):
        try:
            with open(env_file, 'w', encoding='utf-8') as f:
                f.write("# Cấu hình biến môi trường cho ứng dụng Thời tiết Hà Nội\n")
                f.write("# Thay thế các giá trị dưới đây bằng cấu hình của bạn\n\n")
                f.write("# API key OpenWeatherMap\n")
                f.write("OPENWEATHERMAP_API_KEY=your_api_key_here\n")
            
            logger.info(f"Đã tạo file .env mẫu tại {env_file}. Vui lòng cập nhật API key.")
            
            # Thiết lập quyền đúng cho file .env
            try:
                os.chmod(env_file, 0o600)  # Chỉ người sở hữu mới có quyền đọc/ghi
                logger.info("Đã thiết lập quyền truy cập cho file .env.")
            except Exception as e:
                logger.warning(f"Không thể thiết lập quyền truy cập cho file .env: {e}")
                
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tạo file .env mẫu: {e}", exc_info=True)
            return False
    
    return False

# --- Kiểm tra môi trường thực thi ---
def check_environment():
    """Kiểm tra môi trường thực thi và hiển thị thông tin hữu ích."""
    logger = logging.getLogger(__name__)
    
    print("\n=== THÔNG TIN MÔI TRƯỜNG THỰC THI ===")
    
    # Kiểm tra phiên bản Python
    print(f"Python: {sys.version}")
    
    # Kiểm tra thư mục làm việc
    print(f"Thư mục làm việc: {os.getcwd()}")
    print(f"Thư mục script: {SCRIPT_DIR}")
    
    # Kiểm tra đường dẫn tệp
    print(f"Tệp log: {LOG_FILENAME}")
    print(f"Tệp cơ sở dữ liệu: {DB_PATH}")
    
    # Kiểm tra API key
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    if api_key:
        # Che giấu API key khi hiển thị
        masked_key = api_key[:4] + "*" * (len(api_key) - 8) + api_key[-4:]
        print(f"API key: {masked_key} (Đã thiết lập)")
    else:
        print("API key: Chưa thiết lập")
    
    # Kiểm tra quyền ghi vào thư mục làm việc
    try:
        test_file = os.path.join(SCRIPT_DIR, '.write_test')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        print("Quyền ghi vào thư mục: OK")
    except Exception as e:
        print(f"Quyền ghi vào thư mục: LỖI - {e}")
    
    print("=======================================\n")
    
    # Kiểm tra kết nối Internet
    try:
        import requests
        logger.info("Kiểm tra kết nối Internet...")
        response = requests.get("https://www.google.com", timeout=5)
        logger.info("Kết nối Internet: OK")
        print("Kết nối Internet: OK")
    except Exception as e:
        logger.error(f"Kết nối Internet: LỖI - {e}")
        print(f"Kết nối Internet: LỖI - {e}")

# --- Hiển thị thời tiết trên terminal ---
def display_weather(weather_data):
    """Hiển thị thông tin thời tiết dưới dạng đẹp trên terminal"""
    if not weather_data:
        print("Không có dữ liệu thời tiết để hiển thị.")
        return
    
    print("\n=== THỜI TIẾT HÀ NỘI ===")
    print(f"Thành phố: {weather_data['city_name']}")
    print(f"Nhiệt độ: {weather_data['temperature_celsius']}°C")
    print(f"Độ ẩm: {weather_data['humidity_percent']}%")
    print(f"Áp suất: {weather_data['pressure_hpa']} hPa")
    print(f"Tốc độ gió: {weather_data['wind_speed_mps']} m/s")
    print(f"Mô tả: {weather_data['weather_description']}")
    print(f"Cập nhật: {weather_data['timestamp_utc']}")
    print("========================\n")

# --- Hàm Chính ---
def main():
    # Tạo parser dòng lệnh
    parser = argparse.ArgumentParser(description='Ứng dụng thu thập dữ liệu thời tiết Hà Nội')
    parser.add_argument('--setup', '-s', action='store_true', help='Tạo file .env mẫu và kiểm tra môi trường')
    parser.add_argument('--debug', '-d', action='store_true', help='Chạy ở chế độ debug (log chi tiết hơn)')
    parser.add_argument('--city', '-c', type=str, default=CITY, help=f'Thành phố cần lấy dữ liệu thời tiết (mặc định: {CITY})')
    parser.add_argument('--no-save', '-n', action='store_true', help='Không lưu dữ liệu vào cơ sở dữ liệu')
    parser.add_argument('--version', '-v', action='version', version='Hanoi Weather CLI v1.0.0')
    
    # Parse tham số
    args = parser.parse_args()
    
    # Thiết lập logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger = weather_module.setup_logging(LOG_FILENAME, log_level)
    logger.info("=== Bắt đầu chương trình thời tiết Hà Nội CLI ===")
    
    try:
        # Kiểm tra nếu là chế độ setup
        if args.setup:
            create_sample_env_file(ENV_FILE)
            check_environment()
            return
        
        # Tải biến môi trường từ file .env nếu có
        weather_module.load_env_file(ENV_FILE)
        
        # Tạo file .env mẫu nếu chưa tồn tại và chưa có API key
        if not os.getenv('OPENWEATHERMAP_API_KEY') and not os.path.exists(ENV_FILE):
            create_sample_env_file(ENV_FILE)
            logger.warning("API key chưa được thiết lập. Vui lòng cập nhật file .env.")
            print("API key chưa được thiết lập. Vui lòng cập nhật file .env với API key của bạn.")
            return
        
        # Tải cấu hình
        db_path = None if args.no_save else DB_PATH
        config = weather_module.load_config(city=args.city, db_path=db_path)
        api_key = config['api_key']
        city = config['city']
        
        # Lấy dữ liệu thời tiết
        weather_data = weather_module.get_current_weather(api_key, city, db_path=db_path)
        
        # Hiển thị thời tiết
        if weather_data:
            display_weather(weather_data)
        else:
            print("Không thể lấy dữ liệu thời tiết. Vui lòng kiểm tra log để biết chi tiết.")
            
    except KeyError as config_err:
        logger.critical(f"Lỗi cấu hình: {config_err}")
        print(f"Lỗi cấu hình: {config_err}")
    except Exception as e:
        logger.critical(f"Lỗi không xác định: {e}", exc_info=True)
        print(f"Lỗi không xác định: {e}")
    finally:
        logger.info("=== Kết thúc chương trình thời tiết Hà Nội CLI ===")

# --- Điểm Bắt đầu Thực thi ---
if __name__ == "__main__":
    main() 