#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import logging
import logging.handlers
import requests
import json
import sqlite3
import datetime
import time
import sys
from pathlib import Path

# --- Xử lý Đường dẫn Tệp và Thư mục ---
# Lấy thư mục hiện tại của script để đảm bảo đường dẫn tuyệt đối khi chạy tự động
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(SCRIPT_DIR)  # Chuyển thư mục làm việc đến thư mục chứa script

# --- Cấu hình ---
LOG_FILENAME = os.path.join(SCRIPT_DIR, 'hanoi_weather.log')
LOG_LEVEL = logging.INFO
EXPECTED_CITY = "Hanoi"
DB_PATH = os.path.join(SCRIPT_DIR, 'hanoi_weather.db')
API_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"
ENV_FILE = os.path.join(SCRIPT_DIR, '.env')
MAX_RETRIES = 3  # Số lần thử lại tối đa cho các yêu cầu API
RETRY_DELAY = 1  # Thời gian chờ ban đầu giữa các lần thử (giây)

# --- Thiết lập Logging ---
def setup_logging():
    """Cấu hình hệ thống logging."""
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)

    # Xóa tất cả các handlers hiện có để tránh lặp log khi chạy nhiều lần
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    # Tạo RotatingFileHandler để ghi log vào file và xoay vòng
    log_dir = os.path.dirname(LOG_FILENAME)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILENAME, maxBytes=1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Thêm console handler để hiển thị log trên terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info("Logging đã được thiết lập.")

# --- Tải biến môi trường từ file .env (nếu có) ---
def load_env_file():
    """Tải các biến môi trường từ file .env nếu tồn tại."""
    logger = logging.getLogger(__name__)
    
    if os.path.exists(ENV_FILE):
        logger.info(f"Tìm thấy file .env tại {ENV_FILE}. Đang tải biến môi trường...")
        
        try:
            with open(ENV_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip().strip('"\'')
                
            logger.info("Đã tải các biến môi trường từ file .env thành công.")
        except Exception as e:
            logger.error(f"Lỗi khi tải file .env: {e}", exc_info=True)
    else:
        logger.info("Không tìm thấy file .env. Sẽ sử dụng biến môi trường hiện có.")

# --- Tải Cấu hình ---
def load_config():
    """Tải cấu hình từ biến môi trường."""
    logger = logging.getLogger(__name__)
    api_key = os.getenv('OPENWEATHERMAP_API_KEY')
    if not api_key:
        logger.critical("Lỗi: Không tìm thấy biến môi trường OPENWEATHERMAP_API_KEY.")
        logger.info("Hãy đặt OPENWEATHERMAP_API_KEY trong biến môi trường hoặc trong file .env")
        raise KeyError("OPENWEATHERMAP_API_KEY không được thiết lập.")
    
    logger.info("Đã tải cấu hình thành công.")
    return {'api_key': api_key, 'city': EXPECTED_CITY, 'db_path': DB_PATH}

# --- Thực hiện yêu cầu API với cơ chế Retry ---
def fetch_with_retry(url, params, timeout=(5, 10), max_retries=MAX_RETRIES, initial_delay=RETRY_DELAY):
    """
    Thực hiện yêu cầu GET với cơ chế retry và exponential backoff.
    
    Args:
        url: URL endpoint API.
        params: Các tham số query của yêu cầu.
        timeout: Tuple (connect_timeout, read_timeout) tính bằng giây.
        max_retries: Số lần thử lại tối đa.
        initial_delay: Thời gian chờ ban đầu giữa các lần thử (giây).
        
    Returns:
        Response object từ requests.
        
    Raises:
        requests.exceptions.RequestException: Nếu tất cả các lần thử đều thất bại.
    """
    logger = logging.getLogger(__name__)
    retries = 0
    delay = initial_delay
    
    while retries < max_retries:
        try:
            logger.debug(f"Đang gửi yêu cầu đến {url} (lần thử {retries + 1}/{max_retries})")
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()  # Kiểm tra lỗi HTTP 4xx/5xx
            return response  # Trả về response thành công
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as net_err:
            retries += 1
            if retries < max_retries:
                logger.warning(f"Lỗi mạng/timeout: {net_err}, đang thử lại ({retries}/{max_retries}) sau {delay}s...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logger.error(f"Lỗi mạng/timeout liên tục sau {max_retries} lần thử: {net_err}")
                raise
                
        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code
            if status_code >= 500:  # Chỉ thử lại với lỗi server (5xx)
                retries += 1
                if retries < max_retries:
                    logger.warning(f"Lỗi server API ({status_code}), đang thử lại ({retries}/{max_retries}) sau {delay}s...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Lỗi server API liên tục ({status_code}) sau {max_retries} lần thử.")
                    raise
            else:  # Không thử lại với lỗi client (4xx)
                if status_code == 401:
                    logger.error(f"Lỗi xác thực API (401). API key không hợp lệ hoặc chưa được kích hoạt.")
                elif status_code == 404:
                    logger.error(f"Lỗi không tìm thấy (404). Kiểm tra lại tên thành phố hoặc endpoint API.")
                elif status_code == 429:
                    logger.error(f"Lỗi vượt quá giới hạn yêu cầu (429). Hãy giảm tần suất gọi API.")
                else:
                    logger.error(f"Lỗi HTTP không mong đợi: {http_err}")
                raise  # Không thử lại các lỗi 4xx
                
        except json.JSONDecodeError as json_err:
            logger.error(f"Lỗi phân tích JSON: {json_err}")
            raise  # Không thử lại lỗi JSON
            
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Lỗi request không mong đợi: {req_err}")
            raise  # Không thử lại các lỗi request khác
    
    # Nếu đến đây, đã hết số lần thử
    logger.error(f"Không thể lấy dữ liệu sau {max_retries} lần thử.")
    raise requests.exceptions.RequestException(f"Đã đạt số lần thử lại tối đa ({max_retries})")

# --- Thực hiện yêu cầu API ---
def fetch_weather_data(api_key, city):
    """Thực hiện yêu cầu GET tới API OpenWeatherMap để lấy dữ liệu thời tiết."""
    logger = logging.getLogger(__name__)
    logger.info(f"Đang lấy dữ liệu thời tiết cho {city}...")
    
    # Xây dựng tham số URL
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'  # Đảm bảo dữ liệu trả về trong hệ mét (độ C, m/s)
    }
    
    try:
        # Sử dụng hàm fetch_with_retry thay vì requests.get trực tiếp
        response = fetch_with_retry(API_BASE_URL, params)
        
        # Phân tích dữ liệu JSON
        try:
            weather_data = response.json()
            logger.info(f"Đã lấy dữ liệu thời tiết thành công cho {city}.")
            return weather_data
        except json.JSONDecodeError as json_err:
            logger.error(f"Lỗi phân tích JSON từ API: {json_err}", exc_info=True)
            raise
        
    except requests.exceptions.HTTPError as http_err:
        status_code = getattr(http_err.response, 'status_code', None)
        logger.error(f"Lỗi HTTP ({status_code}) khi lấy dữ liệu thời tiết: {http_err}")
        raise
    
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Lỗi timeout khi kết nối đến {API_BASE_URL}: {timeout_err}")
        raise
    
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Lỗi kết nối mạng. Không thể kết nối đến {API_BASE_URL}: {conn_err}")
        raise
    
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Lỗi không xác định khi thực hiện yêu cầu API: {req_err}", exc_info=True)
        raise

# --- Trích xuất Dữ liệu JSON ---
def extract_weather_data(data):
    """Trích xuất dữ liệu thời tiết cần thiết từ dictionary đã phân tích."""
    logger = logging.getLogger(__name__)
    
    try:
        main_data = data.get('main', {})  # Lấy dict 'main', trả về {} nếu không có
        wind_data = data.get('wind', {})  # Lấy dict 'wind', trả về {} nếu không có
        weather_list = data.get('weather', [])  # Lấy list 'weather', trả về [] nếu không có

        temperature = main_data.get('temp')  # Trả về None nếu 'temp' không có
        humidity = main_data.get('humidity')  # Trả về None nếu 'humidity' không có
        pressure = main_data.get('pressure')  # Trả về None nếu 'pressure' không có
        wind_speed = wind_data.get('speed')  # Trả về None nếu 'speed' không có

        # Xử lý danh sách 'weather' an toàn
        weather_description = None
        if weather_list:  # Kiểm tra xem danh sách có phần tử không
            first_weather = weather_list[0]  # Lấy phần tử đầu tiên
            if isinstance(first_weather, dict):  # Kiểm tra xem phần tử có phải dict không
                weather_description = first_weather.get('description')  # Trả về None nếu 'description' không có

        city_name = data.get('name')  # Trả về None nếu 'name' không có

        extracted_data = {
            'temperature': temperature,
            'humidity': humidity,
            'pressure': pressure,
            'wind_speed': wind_speed,
            'description': weather_description,
            'city_name': city_name
        }
        
        logger.info(f"Đã trích xuất dữ liệu thời tiết: {temperature}°C, {humidity}%, {pressure}hPa, {wind_speed}m/s, '{weather_description}'")
        return extracted_data
        
    except (KeyError, IndexError) as e:
        # Ghi log cụ thể cho lỗi truy cập dữ liệu
        logger.error(f"Lỗi truy cập dữ liệu: {e}. Cấu trúc dữ liệu không như mong đợi.", exc_info=True)
        return None
    except Exception as e:
        # Ghi log lỗi nếu có vấn đề không lường trước trong quá trình trích xuất
        logger.error(f"Lỗi không xác định khi trích xuất dữ liệu: {e}", exc_info=True)
        return None  # Hoặc trả về một cấu trúc lỗi

# --- Lấy dấu thời gian UTC ---
def get_utc_timestamp():
    """Lấy dấu thời gian UTC hiện tại theo định dạng ISO 8601."""
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

# --- Xử lý và Xác thực Dữ liệu ---
def process_weather_data(extracted_data, expected_city):
    """Xử lý dữ liệu thô, thêm timestamp và xác thực cơ bản."""
    logger = logging.getLogger(__name__)
    
    if not extracted_data:
        logger.error("Không có dữ liệu trích xuất để xử lý.")
        return None
    
    # Xác thực cơ bản (kiểm tra các giá trị None)
    if None in [extracted_data['temperature'], extracted_data['humidity'], extracted_data['pressure'], extracted_data['wind_speed']]:
        logger.warning("Thiếu dữ liệu quan trọng, bỏ qua bản ghi.")
        return None
    
    # Kiểm tra giá trị có hợp lệ không
    try:
        temp = float(extracted_data['temperature'])
        humidity = int(extracted_data['humidity'])
        pressure = float(extracted_data['pressure'])
        wind_speed = float(extracted_data['wind_speed'])
        
        # Kiểm tra phạm vi giá trị hợp lý
        if temp < -100 or temp > 100:
            logger.warning(f"Nhiệt độ ({temp}°C) nằm ngoài phạm vi hợp lý, có thể không chính xác.")
        if humidity < 0 or humidity > 100:
            logger.warning(f"Độ ẩm ({humidity}%) nằm ngoài phạm vi hợp lý (0-100).")
        if pressure < 800 or pressure > 1200:
            logger.warning(f"Áp suất ({pressure}hPa) nằm ngoài phạm vi hợp lý, có thể không chính xác.")
        if wind_speed < 0 or wind_speed > 200:
            logger.warning(f"Tốc độ gió ({wind_speed}m/s) nằm ngoài phạm vi hợp lý, có thể không chính xác.")
            
    except (ValueError, TypeError) as e:
        logger.error(f"Lỗi chuyển đổi dữ liệu: {e}. Định dạng giá trị không hợp lệ.")
        return None
    
    # Xác minh tên thành phố
    if extracted_data['city_name'] != expected_city:
        logger.warning(f"Dữ liệu trả về cho thành phố '{extracted_data['city_name']}', không phải '{expected_city}'.")
    
    # Định dạng dữ liệu với đơn vị rõ ràng
    processed_data = {
        'timestamp_utc': get_utc_timestamp(),  # Thêm timestamp UTC ISO8601
        'city_name': extracted_data['city_name'] if extracted_data['city_name'] else expected_city,
        'temperature_celsius': temp,
        'humidity_percent': humidity,
        'pressure_hpa': pressure,
        'wind_speed_mps': wind_speed,
        'weather_description': extracted_data['description']
    }
    
    logger.info(f"Đã xử lý dữ liệu thời tiết: {processed_data['temperature_celsius']}°C, {processed_data['humidity_percent']}%, {processed_data['weather_description']}")
    return processed_data

# --- Lưu dữ liệu vào SQLite ---
def save_to_sqlite(data_dict, db_path):
    """Lưu dữ liệu thời tiết vào SQLite."""
    logger = logging.getLogger(__name__)
    
    # SQL để tạo bảng nếu chưa tồn tại
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_log (
        timestamp_utc TEXT PRIMARY KEY,
        city_name TEXT NOT NULL,
        temperature_celsius REAL,
        humidity_percent INTEGER,
        pressure_hpa REAL,
        wind_speed_mps REAL,
        weather_description TEXT
    );
    """
    
    # SQL để tạo index
    create_index_sql = """
    CREATE INDEX IF NOT EXISTS idx_timestamp ON weather_log (timestamp_utc);
    """
    
    # SQL để chèn dữ liệu
    insert_sql = """
    INSERT INTO weather_log (
        timestamp_utc, city_name, temperature_celsius, humidity_percent,
        pressure_hpa, wind_speed_mps, weather_description
    ) VALUES (?,?,?,?,?,?,?);
    """
    
    # Chuyển đổi dictionary thành tuple theo đúng thứ tự cột
    data_tuple = (
        data_dict['timestamp_utc'],
        data_dict['city_name'],
        data_dict['temperature_celsius'],
        data_dict['humidity_percent'],
        data_dict['pressure_hpa'],
        data_dict['wind_speed_mps'],
        data_dict['weather_description']
    )
    
    try:
        # Kiểm tra xem thư mục chứa database có tồn tại không
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir)
            logger.info(f"Đã tạo thư mục {db_dir} để lưu trữ cơ sở dữ liệu.")
            
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # Tạo bảng và index nếu chưa tồn tại
            cursor.execute(create_table_sql)
            cursor.execute(create_index_sql)
            
            # Chèn dữ liệu
            cursor.execute(insert_sql, data_tuple)
            
            # Lưu thay đổi
            conn.commit()
            
            logger.info(f"Đã lưu dữ liệu thành công vào SQLite lúc {data_dict['timestamp_utc']}")
            return True
            
    except sqlite3.IntegrityError:
        logger.warning(f"Dữ liệu với timestamp {data_dict['timestamp_utc']} đã tồn tại.")
        return False
        
    except sqlite3.Error as e:
        logger.error(f"Lỗi khi lưu vào SQLite: {e}", exc_info=True)
        # Kiểm tra quyền truy cập
        try:
            with open(os.path.join(SCRIPT_DIR, '.db_write_test'), 'w') as f:
                f.write('test')
            os.remove(os.path.join(SCRIPT_DIR, '.db_write_test'))
            logger.info("Có quyền ghi vào thư mục cơ sở dữ liệu.")
        except IOError as io_err:
            logger.error(f"Có thể không có quyền ghi vào thư mục: {io_err}")
        except Exception as ex:
            logger.error(f"Lỗi không xác định khi kiểm tra quyền ghi: {ex}")
        raise
    
    except Exception as e:
        logger.critical(f"Lỗi không xác định khi lưu dữ liệu: {e}", exc_info=True)
        raise

# --- Kiểm tra API ---
def check_api_connection(api_key):
    """Kiểm tra kết nối tới API OpenWeatherMap."""
    logger = logging.getLogger(__name__)
    logger.info("Kiểm tra kết nối API OpenWeatherMap...")
    
    params = {
        'q': EXPECTED_CITY,
        'appid': api_key,
        'units': 'metric'
    }
    
    try:
        # Sử dụng hàm fetch_with_retry với số lần thử ít hơn cho kiểm tra nhanh
        response = fetch_with_retry(API_BASE_URL, params, max_retries=2)
        status_code = response.status_code
        
        if status_code == 200:
            logger.info(f"Kết nối API thành công! Mã trạng thái: {status_code}")
            return True
            
    except requests.exceptions.HTTPError as http_err:
        status_code = http_err.response.status_code
        if status_code == 401:
            logger.error(f"Lỗi xác thực API (401). API key không hợp lệ hoặc chưa được kích hoạt.")
            logger.info("Lưu ý: API key mới có thể cần từ 10 phút đến 2 giờ để kích hoạt.")
        elif status_code == 404:
            logger.error(f"Lỗi không tìm thấy (404). Kiểm tra lại endpoint API hoặc tên thành phố.")
        elif status_code == 429:
            logger.error(f"Lỗi vượt quá giới hạn yêu cầu (429). Hãy thử lại sau.")
        else:
            logger.error(f"Lỗi HTTP không xác định. Mã trạng thái: {status_code}")
        return False
            
    except requests.exceptions.ConnectionError:
        logger.error("Lỗi kết nối mạng. Không thể kết nối đến API.")
        return False
    except requests.exceptions.Timeout:
        logger.error("Lỗi timeout. API không phản hồi.")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi request: {e}")
        return False
    except Exception as e:
        logger.error(f"Lỗi không xác định khi kiểm tra API: {e}", exc_info=True)
        return False
        
    # Nếu không có lỗi nhưng có vấn đề khác
    logger.error(f"Kiểm tra API không thành công. Mã trạng thái: {status_code}")
    return False

# --- Tạo file .env mẫu nếu không tồn tại ---
def create_sample_env_file():
    """Tạo file .env mẫu nếu chưa tồn tại."""
    logger = logging.getLogger(__name__)
    
    if not os.path.exists(ENV_FILE):
        try:
            with open(ENV_FILE, 'w', encoding='utf-8') as f:
                f.write("# Cấu hình biến môi trường cho ứng dụng Thời tiết Hà Nội\n")
                f.write("# Thay thế các giá trị dưới đây bằng cấu hình của bạn\n\n")
                f.write("# API key OpenWeatherMap\n")
                f.write("OPENWEATHERMAP_API_KEY=your_api_key_here\n")
            
            logger.info(f"Đã tạo file .env mẫu tại {ENV_FILE}. Vui lòng cập nhật API key.")
            
            # Thiết lập quyền đúng cho file .env
            try:
                os.chmod(ENV_FILE, 0o600)  # Chỉ người sở hữu mới có quyền đọc/ghi
                logger.info("Đã thiết lập quyền truy cập cho file .env.")
            except Exception as e:
                logger.warning(f"Không thể thiết lập quyền truy cập cho file .env: {e}")
                
        except Exception as e:
            logger.error(f"Lỗi khi tạo file .env mẫu: {e}", exc_info=True)

# --- Hiển thị thông tin trợ giúp ---
def show_help():
    print(f"""
Sử dụng: python {os.path.basename(__file__)} [tùy chọn]

Tùy chọn:
  --help, -h     : Hiển thị thông tin trợ giúp này
  --setup, -s    : Tạo file .env mẫu và kiểm tra môi trường
  --debug, -d    : Chạy ở chế độ debug (log chi tiết hơn)

Ví dụ:
  python {os.path.basename(__file__)}           # Chạy chương trình thông thường
  python {os.path.basename(__file__)} --setup   # Thiết lập môi trường
  python {os.path.basename(__file__)} --help    # Hiển thị trợ giúp
  python {os.path.basename(__file__)} --debug   # Chạy với log chi tiết

Lưu ý:
  - Cần thiết lập API key OpenWeatherMap trong biến môi trường OPENWEATHERMAP_API_KEY
    hoặc trong file .env trước khi chạy.
  - Khi chạy tự động qua Task Scheduler (Windows) hoặc cron (Linux/macOS),
    nên sử dụng đường dẫn tuyệt đối đến Python và đến script này.
""")

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
        logger.info("Kiểm tra kết nối Internet...")
        response = requests.get("https://www.google.com", timeout=5)
        logger.info("Kết nối Internet: OK")
    except requests.exceptions.RequestException as e:
        logger.error(f"Kết nối Internet: LỖI - {e}")

# --- Hàm Chính ---
def main():
    logger = logging.getLogger(__name__)
    logger.info("=== Bắt đầu chương trình thời tiết Hà Nội ===")
    
    try:
        # Kiểm tra tham số dòng lệnh
        if len(sys.argv) > 1:
            if sys.argv[1] in ('--help', '-h'):
                show_help()
                return
            elif sys.argv[1] in ('--setup', '-s'):
                create_sample_env_file()
                check_environment()
                return
            elif sys.argv[1] in ('--debug', '-d'):
                # Thay đổi mức log thành DEBUG
                logging.getLogger().setLevel(logging.DEBUG)
                logger.info("Đã bật chế độ DEBUG.")
        
        # Tải biến môi trường từ file .env nếu có
        load_env_file()
        
        # Tạo file .env mẫu nếu chưa tồn tại và chưa có API key
        if not os.getenv('OPENWEATHERMAP_API_KEY') and not os.path.exists(ENV_FILE):
            create_sample_env_file()
            logger.warning("API key chưa được thiết lập. Vui lòng cập nhật file .env.")
        
        # Tải cấu hình
        config = load_config()
        api_key = config['api_key']
        city = config['city']
        db_path = config['db_path']
        
        # Kiểm tra kết nối API
        api_status = check_api_connection(api_key)
        
        if api_status:
            logger.info("API đã sẵn sàng để sử dụng!")
            
            # Lấy dữ liệu thời tiết
            weather_data = fetch_weather_data(api_key, city)
            
            # Trích xuất dữ liệu quan trọng
            extracted_data = extract_weather_data(weather_data)
            
            # Xử lý và xác thực dữ liệu
            if extracted_data:
                processed_data = process_weather_data(extracted_data, city)
                if processed_data:
                    # Lưu dữ liệu vào SQLite
                    save_result = save_to_sqlite(processed_data, db_path)
                    if save_result:
                        logger.info(f"Đã lưu thành công dữ liệu thời tiết cho {city} vào cơ sở dữ liệu.")
                    else:
                        logger.warning("Không lưu dữ liệu do có lỗi hoặc dữ liệu đã tồn tại.")
                        
                    logger.info(f"Thời tiết hiện tại ở {city}: {processed_data['temperature_celsius']}°C, độ ẩm {processed_data['humidity_percent']}%, {processed_data['weather_description']}")
                else:
                    logger.warning("Dữ liệu sau khi xử lý không hợp lệ, không tiếp tục xử lý.")
            else:
                logger.warning("Không thể trích xuất dữ liệu thời tiết từ phản hồi API.")
        else:
            logger.warning("Kiểm tra API không thành công. Xem log để biết chi tiết.")
            
    except KeyError as config_err:
        logger.critical(f"Lỗi cấu hình: {config_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Lỗi khi thực hiện yêu cầu API: {req_err}")
    except sqlite3.Error as db_err:
        logger.error(f"Lỗi cơ sở dữ liệu SQLite: {db_err}", exc_info=True)
    except Exception as e:
        logger.critical(f"Lỗi không xác định: {e}", exc_info=True)
    finally:
        logger.info("=== Kết thúc chương trình thời tiết Hà Nội ===")

# --- Điểm Bắt đầu Thực thi ---
if __name__ == "__main__":
    setup_logging()
    main()
