#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Module chứa các chức năng thu thập dữ liệu thời tiết từ OpenWeatherMap API.

Module này cung cấp các hàm để:
1. Lấy dữ liệu thời tiết từ API OpenWeatherMap
2. Xử lý và xác thực dữ liệu
3. Lưu trữ dữ liệu vào cơ sở dữ liệu SQLite
"""

import os
import logging
import logging.handlers
import requests
import json
import sqlite3
import datetime
import time
from pathlib import Path

# --- Cấu hình Mặc định ---
EXPECTED_CITY = "Hanoi"
MAX_RETRIES = 3  # Số lần thử lại tối đa cho các yêu cầu API
RETRY_DELAY = 1  # Thời gian chờ ban đầu giữa các lần thử (giây)
API_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# --- Hàm Thiết lập Logging ---
def setup_logging(log_filename, log_level=logging.INFO):
    """
    Cấu hình hệ thống logging với file handler và console handler.
    
    Args:
        log_filename (str): Đường dẫn tới file log
        log_level (int): Mức độ log (mặc định: logging.INFO)
        
    Returns:
        logger: Logger object đã được cấu hình
    """
    logger = logging.getLogger()
    logger.setLevel(log_level)

    # Xóa tất cả các handlers hiện có để tránh lặp log khi chạy nhiều lần
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    # Tạo RotatingFileHandler để ghi log vào file và xoay vòng
    log_dir = os.path.dirname(log_filename)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)

    file_handler = logging.handlers.RotatingFileHandler(
        log_filename, maxBytes=1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Thêm console handler để hiển thị log trên terminal
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info("Logging đã được thiết lập.")
    return logger

# --- Tải biến môi trường từ file .env ---
def load_env_file(env_file):
    """
    Tải các biến môi trường từ file .env nếu tồn tại.
    
    Args:
        env_file (str): Đường dẫn tới file .env
        
    Returns:
        bool: True nếu tải thành công, False nếu có lỗi
    """
    logger = logging.getLogger(__name__)
    
    if os.path.exists(env_file):
        logger.info(f"Tìm thấy file .env tại {env_file}. Đang tải biến môi trường...")
        
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip().strip('"\'')
                
            logger.info("Đã tải các biến môi trường từ file .env thành công.")
            return True
        except Exception as e:
            logger.error(f"Lỗi khi tải file .env: {e}", exc_info=True)
            return False
    else:
        logger.info("Không tìm thấy file .env. Sẽ sử dụng biến môi trường hiện có.")
        return False

# --- Tải Cấu hình ---
def load_config(api_key_env_var='OPENWEATHERMAP_API_KEY', city=EXPECTED_CITY, db_path=None):
    """
    Tải cấu hình từ biến môi trường.
    
    Args:
        api_key_env_var (str): Tên biến môi trường chứa API key
        city (str): Tên thành phố cần lấy dữ liệu thời tiết
        db_path (str): Đường dẫn tới file cơ sở dữ liệu SQLite
        
    Returns:
        dict: Dictionary chứa các thông tin cấu hình
        
    Raises:
        KeyError: Nếu không tìm thấy API key
    """
    logger = logging.getLogger(__name__)
    api_key = os.getenv(api_key_env_var)
    if not api_key:
        logger.critical(f"Lỗi: Không tìm thấy biến môi trường {api_key_env_var}.")
        logger.info(f"Hãy đặt {api_key_env_var} trong biến môi trường hoặc trong file .env")
        raise KeyError(f"{api_key_env_var} không được thiết lập.")
    
    logger.info("Đã tải cấu hình thành công.")
    return {'api_key': api_key, 'city': city, 'db_path': db_path}

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
def fetch_weather_data(api_key, city, api_url=API_BASE_URL):
    """
    Thực hiện yêu cầu GET tới API OpenWeatherMap để lấy dữ liệu thời tiết.
    
    Args:
        api_key (str): API key OpenWeatherMap
        city (str): Tên thành phố cần lấy dữ liệu thời tiết
        api_url (str): URL endpoint API
        
    Returns:
        dict: Dữ liệu thời tiết dạng JSON
        
    Raises:
        requests.exceptions.HTTPError: Nếu có lỗi HTTP
        json.JSONDecodeError: Nếu có lỗi phân tích JSON
        requests.exceptions.RequestException: Nếu có lỗi request khác
    """
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
        response = fetch_with_retry(api_url, params)
        
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
        logger.error(f"Lỗi timeout khi kết nối đến {api_url}: {timeout_err}")
        raise
    
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Lỗi kết nối mạng. Không thể kết nối đến {api_url}: {conn_err}")
        raise
    
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Lỗi không xác định khi thực hiện yêu cầu API: {req_err}", exc_info=True)
        raise

# --- Trích xuất Dữ liệu JSON ---
def extract_weather_data(data):
    """
    Trích xuất dữ liệu thời tiết cần thiết từ dictionary đã phân tích.
    
    Args:
        data (dict): Dữ liệu thời tiết dạng JSON
        
    Returns:
        dict: Dictionary chứa các thông tin thời tiết đã trích xuất
    """
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
        return None

# --- Lấy dấu thời gian UTC ---
def get_utc_timestamp():
    """
    Lấy dấu thời gian UTC hiện tại theo định dạng ISO 8601.
    
    Returns:
        str: Dấu thời gian UTC định dạng ISO 8601
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

# --- Xử lý và Xác thực Dữ liệu ---
def process_weather_data(extracted_data, expected_city):
    """
    Xử lý dữ liệu thô, thêm timestamp và xác thực cơ bản.
    
    Args:
        extracted_data (dict): Dữ liệu thời tiết đã trích xuất
        expected_city (str): Tên thành phố cần kiểm tra
        
    Returns:
        dict: Dictionary chứa dữ liệu thời tiết đã xử lý
    """
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
    """
    Lưu dữ liệu thời tiết vào SQLite.
    
    Args:
        data_dict (dict): Dictionary chứa dữ liệu thời tiết đã xử lý
        db_path (str): Đường dẫn tới file cơ sở dữ liệu SQLite
        
    Returns:
        bool: True nếu lưu thành công, False nếu có lỗi
        
    Raises:
        sqlite3.Error: Nếu có lỗi khi làm việc với SQLite
    """
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
        raise
    
    except Exception as e:
        logger.critical(f"Lỗi không xác định khi lưu dữ liệu: {e}", exc_info=True)
        raise

# --- Kiểm tra API ---
def check_api_connection(api_key, api_url=API_BASE_URL, city=EXPECTED_CITY, max_retries=2):
    """
    Kiểm tra kết nối tới API OpenWeatherMap.
    
    Args:
        api_key (str): API key OpenWeatherMap
        api_url (str): URL endpoint API
        city (str): Tên thành phố cần kiểm tra
        max_retries (int): Số lần thử lại tối đa
        
    Returns:
        bool: True nếu kết nối thành công, False nếu có lỗi
    """
    logger = logging.getLogger(__name__)
    logger.info("Kiểm tra kết nối API OpenWeatherMap...")
    
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric'
    }
    
    try:
        # Sử dụng hàm fetch_with_retry với số lần thử ít hơn cho kiểm tra nhanh
        response = fetch_with_retry(api_url, params, max_retries=max_retries)
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

# --- Lấy thời tiết hiện tại ---
def get_current_weather(api_key, city=EXPECTED_CITY, api_url=API_BASE_URL, db_path=None):
    """
    Lấy thông tin thời tiết hiện tại cho một thành phố và lưu vào cơ sở dữ liệu.
    
    Args:
        api_key (str): API key OpenWeatherMap
        city (str): Tên thành phố cần lấy dữ liệu thời tiết
        api_url (str): URL endpoint API
        db_path (str): Đường dẫn tới file cơ sở dữ liệu SQLite
        
    Returns:
        dict: Dictionary chứa dữ liệu thời tiết đã xử lý hoặc None nếu có lỗi
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Kiểm tra kết nối API
        if not check_api_connection(api_key, api_url, city):
            logger.warning("Kiểm tra API không thành công. Không tiếp tục.")
            return None
        
        # Lấy dữ liệu thời tiết
        weather_data = fetch_weather_data(api_key, city, api_url)
        
        # Trích xuất dữ liệu quan trọng
        extracted_data = extract_weather_data(weather_data)
        
        if not extracted_data:
            logger.warning("Không thể trích xuất dữ liệu thời tiết từ phản hồi API.")
            return None
        
        # Xử lý và xác thực dữ liệu
        processed_data = process_weather_data(extracted_data, city)
        
        if not processed_data:
            logger.warning("Dữ liệu sau khi xử lý không hợp lệ.")
            return None
        
        # Lưu dữ liệu vào SQLite nếu có đường dẫn db
        if db_path:
            try:
                save_result = save_to_sqlite(processed_data, db_path)
                if save_result:
                    logger.info(f"Đã lưu thành công dữ liệu thời tiết cho {city} vào cơ sở dữ liệu.")
                else:
                    logger.warning("Không lưu dữ liệu do có lỗi hoặc dữ liệu đã tồn tại.")
            except sqlite3.Error as e:
                logger.error(f"Lỗi cơ sở dữ liệu: {e}", exc_info=True)
                # Tiếp tục trả về dữ liệu thời tiết ngay cả khi không lưu được
        
        return processed_data
        
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Lỗi khi thực hiện yêu cầu API: {req_err}")
    except KeyError as key_err:
        logger.error(f"Lỗi khóa không tồn tại: {key_err}")
    except Exception as e:
        logger.critical(f"Lỗi không xác định: {e}", exc_info=True)
    
    return None 