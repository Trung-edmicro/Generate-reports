import os
import re
import time
import asyncio
import pandas as pd
import google.generativeai as genai
from dotenv import load_dotenv
from logger_config import logger
from utils.helpers import load_prompt
from multiprocessing import Pool, Queue, Manager
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
from datetime import datetime, timedelta
import threading
from collections import defaultdict
import json
from google.oauth2 import service_account

load_dotenv()

# Load all API keys
API_KEYS = []
for i in range(1, 22):
    api_key = os.getenv(f"API_KEY_{i}")
    if api_key:
        API_KEYS.append(api_key)

# Load Service Account configuration
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
SERVICE_ACCOUNT_KEY_JSON = os.getenv("SERVICE_ACCOUNT_KEY_JSON")

# Initialize service account credentials
service_account_credentials = None
if SERVICE_ACCOUNT_KEY_JSON:
    try:
        service_account_info = json.loads(SERVICE_ACCOUNT_KEY_JSON)
        service_account_credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=[
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/generative-language'
            ]
        )
        logger.info("✅ Đã tải Service Account từ JSON string")
    except Exception as e:
        logger.error(f"❌ Lỗi khi tải Service Account từ JSON: {e}")
elif SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
    try:
        service_account_credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=[
                'https://www.googleapis.com/auth/cloud-platform',
                'https://www.googleapis.com/auth/generative-language'
            ]
        )
        logger.info(f"✅ Đã tải Service Account từ file: {SERVICE_ACCOUNT_FILE}")
    except Exception as e:
        logger.error(f"❌ Lỗi khi tải Service Account từ file: {e}")
else:
    # Thử tạo service account từ env vars riêng lẻ
    project_id = os.getenv('PROJECT_ID')
    private_key = os.getenv('PRIVATE_KEY')
    client_email = os.getenv('CLIENT_EMAIL')
    
    if project_id and private_key and client_email:
        try:
            # Tạo service account info từ env vars
            service_account_info = {
                "type": os.getenv('TYPE', 'service_account'),
                "project_id": project_id,
                "private_key_id": os.getenv('PRIVATE_KEY_ID'),
                "private_key": private_key.replace('\\n', '\n'),  # Fix newlines
                "client_email": client_email,
                "client_id": os.getenv('CLIENT_ID', ''),
                "auth_uri": os.getenv('AUTH_URI', 'https://accounts.google.com/o/oauth2/auth'),
                "token_uri": os.getenv('TOKEN_URI', 'https://oauth2.googleapis.com/token'),
                "auth_provider_x509_cert_url": os.getenv('AUTH_PROVIDER_X509_CERT_URL'),
                "client_x509_cert_url": os.getenv('CLIENT_X509_CERT_URL'),
                "universe_domain": os.getenv('UNIVERSE_DOMAIN', 'googleapis.com')
            }
            
            service_account_credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/generative-language'
                ]
            )
            logger.info("✅ Đã tạo Service Account từ environment variables")
        except Exception as e:
            logger.error(f"❌ Lỗi khi tạo Service Account từ env vars: {e}")

if not API_KEYS and not service_account_credentials:
    raise ValueError("Vui lòng đặt biến môi trường API_KEY hoặc SERVICE_ACCOUNT.")

print(f"Đã tải được {len(API_KEYS)} API keys và {'có' if service_account_credentials else 'không có'} Service Account để xử lý")

# Quản lý trạng thái API keys và Service Account
class APIKeyManager:
    def __init__(self, api_keys, service_account_creds=None):
        self.api_keys = api_keys.copy() if api_keys else []
        self.service_account_creds = service_account_creds
        self.invalid_keys = set()  # Keys bị lỗi 400
        self.key_request_count = defaultdict(list)  # Lưu thời gian request cho mỗi key
        self.key_retry_until = {}  # Thời gian retry cho key bị rate limit
        self.consecutive_rate_limits = 0  # Đếm số lần liên tiếp tất cả key bị rate limit
        self.using_service_account = False
        self.service_account_request_count = []  # Lưu thời gian request cho Service Account
        self.service_account_retry_until = None
        self.lock = threading.Lock()
        
    def get_available_key(self):
        """Lấy key khả dụng hoặc chuyển sang Service Account"""
        with self.lock:
            current_time = datetime.now()
            
            # Kiểm tra các API keys trước
            for key in self.api_keys:
                if key in self.invalid_keys:
                    continue
                    
                # Kiểm tra retry delay
                if key in self.key_retry_until and current_time < self.key_retry_until[key]:
                    continue
                    
                # Kiểm tra số request trong 10 phút qua (15 req/10min limit)
                ten_minutes_ago = current_time - timedelta(minutes=10)
                recent_requests = [req_time for req_time in self.key_request_count[key] if req_time > ten_minutes_ago]
                self.key_request_count[key] = recent_requests  # Cleanup old requests
                
                if len(recent_requests) < 15:  # Chưa đạt giới hạn 15 req/10min
                    self.key_request_count[key].append(current_time)
                    return ("api_key", key)
            
            # Nếu không có API key nào khả dụng, thử Service Account
            if self.service_account_creds:
                # Kiểm tra retry delay của Service Account
                if self.service_account_retry_until and current_time < self.service_account_retry_until:
                    return None
                    
                # Kiểm tra số request trong 1 phút qua (60 req/min limit cho Service Account)
                one_minute_ago = current_time - timedelta(minutes=1)
                recent_sa_requests = [req_time for req_time in self.service_account_request_count if req_time > one_minute_ago]
                self.service_account_request_count = recent_sa_requests  # Cleanup old requests
                
                if len(recent_sa_requests) < 60:  # Service Account limit: 60 req/min
                    self.service_account_request_count.append(current_time)
                    if not self.using_service_account:
                        self.using_service_account = True
                        logger.warning("🔄 Chuyển sang sử dụng Service Account do hết API keys")
                    return ("service_account", self.service_account_creds)
                    
            return None
            
    def mark_key_invalid(self, key):
        """Đánh dấu key bị lỗi 400 (expired/invalid)"""
        with self.lock:
            self.invalid_keys.add(key)
            logger.warning(f"🚫 Key bị đánh dấu invalid: {key[:20]}...")
            
    def mark_key_rate_limited(self, key_type, key_or_creds, retry_delay_seconds=None):
        """Đánh dấu key hoặc Service Account bị rate limit"""
        with self.lock:
            if key_type == "api_key":
                if retry_delay_seconds:
                    retry_until = datetime.now() + timedelta(seconds=retry_delay_seconds)
                    self.key_retry_until[key_or_creds] = retry_until
                    logger.warning(f"⏰ API Key bị rate limit, retry sau {retry_delay_seconds}s: {key_or_creds[:20]}...")
                else:
                    retry_until = datetime.now() + timedelta(seconds=60)
                    self.key_retry_until[key_or_creds] = retry_until
                    logger.warning(f"⏰ API Key bị rate limit, retry sau 60s: {key_or_creds[:20]}...")
                    
            elif key_type == "service_account":
                if retry_delay_seconds:
                    self.service_account_retry_until = datetime.now() + timedelta(seconds=retry_delay_seconds)
                    logger.warning(f"⏰ Service Account bị rate limit, retry sau {retry_delay_seconds}s")
                else:
                    self.service_account_retry_until = datetime.now() + timedelta(seconds=60)
                    logger.warning(f"⏰ Service Account bị rate limit, retry sau 60s")
                
    def check_all_keys_exhausted(self):
        """Kiểm tra tất cả key có bị exhausted không"""
        with self.lock:
            available = self.get_available_key()
            if available is None:
                self.consecutive_rate_limits += 1
                logger.warning(f"⚠️ Tất cả keys và Service Account đều bị rate limit (lần {self.consecutive_rate_limits}/10)")
                return self.consecutive_rate_limits >= 10
            else:
                self.consecutive_rate_limits = 0  # Reset counter khi có key available
                return False
                
    def get_stats(self):
        """Lấy thống kê trạng thái keys"""
        with self.lock:
            total_keys = len(self.api_keys)
            invalid_keys = len(self.invalid_keys)
            current_time = datetime.now()
            rate_limited_keys = sum(1 for key in self.api_keys 
                                  if key in self.key_retry_until and current_time < self.key_retry_until[key])
            available_keys = total_keys - invalid_keys - rate_limited_keys
            
            sa_available = False
            if self.service_account_creds:
                sa_available = (not self.service_account_retry_until or 
                              current_time >= self.service_account_retry_until)
            
            return {
                'total_keys': total_keys,
                'invalid_keys': invalid_keys,
                'rate_limited_keys': rate_limited_keys,
                'available_keys': available_keys,
                'service_account_available': sa_available,
                'using_service_account': self.using_service_account
            }

# Global key manager
key_manager = APIKeyManager(API_KEYS, service_account_credentials)

def create_service_account_credentials():
    """Tạo Service Account credentials từ env vars (dành cho child processes)"""
    try:
        # Thử từ JSON string hoặc file trước
        SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
        SERVICE_ACCOUNT_KEY_JSON = os.getenv("SERVICE_ACCOUNT_KEY_JSON")
        
        if SERVICE_ACCOUNT_KEY_JSON:
            service_account_info = json.loads(SERVICE_ACCOUNT_KEY_JSON)
            return service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/generative-language'
                ]
            )
        elif SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
            return service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=[
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/generative-language'
                ]
            )
        else:
            # Tạo từ env vars riêng lẻ
            project_id = os.getenv('PROJECT_ID')
            private_key = os.getenv('PRIVATE_KEY')
            client_email = os.getenv('CLIENT_EMAIL')
            
            if not all([project_id, private_key, client_email]):
                return None
                
            service_account_info = {
                "type": os.getenv('TYPE', 'service_account'),
                "project_id": project_id,
                "private_key_id": os.getenv('PRIVATE_KEY_ID'),
                "private_key": private_key.replace('\\n', '\n'),
                "client_email": client_email,
                "client_id": os.getenv('CLIENT_ID', ''),
                "auth_uri": os.getenv('AUTH_URI', 'https://accounts.google.com/o/oauth2/auth'),
                "token_uri": os.getenv('TOKEN_URI', 'https://oauth2.googleapis.com/token'),
                "auth_provider_x509_cert_url": os.getenv('AUTH_PROVIDER_X509_CERT_URL'),
                "client_x509_cert_url": os.getenv('CLIENT_X509_CERT_URL'),
                "universe_domain": os.getenv('UNIVERSE_DOMAIN', 'googleapis.com')
            }
            
            return service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/generative-language'
                ]
            )
    except Exception as e:
        logger.error(f"❌ Lỗi tạo Service Account credentials: {e}")
        return None

def reset_key_manager():
    """Reset key manager để sử dụng lại tất cả keys"""
    global key_manager
    key_manager = APIKeyManager(API_KEYS, service_account_credentials)
    logger.info("🔄 Đã reset API key manager")

def extract_retry_delay(error_message):
    """Trích xuất retry_delay từ error message"""
    try:
        # Tìm "retry_delay" trong message
        import re
        
        # Pattern 1: "Please retry in X.Xs"
        pattern1 = r"Please retry in (\d+(?:\.\d+)?)s"
        match1 = re.search(pattern1, str(error_message))
        if match1:
            return float(match1.group(1))
            
        # Pattern 2: "retry_delay { seconds: X }"
        pattern2 = r"retry_delay\s*{\s*seconds:\s*(\d+)"
        match2 = re.search(pattern2, str(error_message))
        if match2:
            return float(match2.group(1))
            
    except Exception as e:
        logger.error(f"Lỗi khi trích xuất retry_delay: {e}")
        
    return None

def handle_sheet(df1, df2):
    # Skip header rows if needed
    if 'STT' in df1.columns and pd.notna(df1.iloc[0]['STT']) and df1.iloc[0]['STT'] == 'STT':
        df1 = df1.iloc[1:].reset_index(drop=True)
    
    # Initialize list for incorrect questions
    result_indices = []
    improvement_content = []
    
    total_basic = (df2["Cấp độ nhận thức"].isin(["NB", "TH", "NBT"])).sum()
    total_advanced = (df2["Cấp độ nhận thức"].isin(["VD", "VDT", "VDC"])).sum()

    correct_list = []
    wrong_list = []
    skip_list = []
    wrong_questions_string_list = []

    basic_percent_list = []
    advanced_percent_list = []
    
    for i, col in enumerate(df1.columns):
        if i >= len(df1.iloc[1]):
            break
        if pd.notna(df1.iloc[1][col]):
            val = str(df1.iloc[1][col]).strip()
            val = val.replace("Ð", "Đ")  # Chuẩn hóa ký tự do xử lý đáp án câu Đúng/Sai bằng VBA trên Excel (tạm thời)

            if val in ['Đúng', 'Sai', 'Bỏ qua']:
                result_indices.append(i)
                
    # Process each student
    for index, row in df1.iterrows():
        wrong_questions = []
        wrong_questions_list = []
        skipped = []

        correct_count = 0
        wrong_count = 0
        skipped_count = 0

        correct_basic = 0
        correct_advanced = 0

        # Check each result column
        for i, col_idx in enumerate(result_indices):
            col = df1.columns[col_idx]
            if pd.notna(row[col]):
                result = row[col].replace("Ð", "Đ")

                if result == 'Đúng':
                    correct_count += 1
                    question_num = i + 1  # Xác định số thứ tự câu hỏi
                    matched_row = df2[df2["Câu hỏi"] == question_num]  # Tìm trong df2
                    
                    if not matched_row.empty:
                        level = matched_row["Cấp độ nhận thức"].values[0]
                        if level in ["NB", "TH", "NBT"]:
                            correct_basic += 1
                        elif level in ["VD", "VDT", "VDC"]:
                            correct_advanced += 1
                elif result == 'Sai':
                    wrong_count += 1
                    question_number = str(i + 1)
                    wrong_questions.append(question_number)
                    wrong_questions_list.append(f"Câu {question_number}")
                elif result == 'Bỏ qua':
                    skipped_count += 1
                    skipped.append(str(i + 1))
            else:
                print(f"Error: if pd.notna(row[col]) is False, {result}")

        # Tạo chuỗi câu sai theo format yêu cầu
        wrong_questions_string = ", ".join(wrong_questions_list) if wrong_questions_list else "Không có"
        wrong_questions_string_list.append(wrong_questions_string)

        # Xử lý tỉ lệ đúng cho từng cấp độ
        percent_basic = f"{int(round((correct_basic / total_basic) * 100, 0))}%" if total_basic > 0 else "0%"
        percent_advanced = f"{int(round((correct_advanced / total_advanced) * 100, 0))}%" if total_advanced > 0 else "0%"

        correct_list.append(correct_count)
        wrong_list.append(wrong_count)
        skip_list.append(skipped_count)

        basic_percent_list.append(percent_basic)
        advanced_percent_list.append(percent_advanced)

        # Xử lý nội dung cần cải thiện từ file câu hỏi (input2)
        combined_questions = set(filter(None, wrong_questions + skipped))
        grouped_dict = {}

        for q in combined_questions:
            if q.isdigit():
                q_int = int(q)
                matched_rows = df2[df2["Câu hỏi"] == q_int]

                # print(f"DEBUG: q={q}, matched_rows.empty={matched_rows.empty}")
                # print(f"DEBUG: q={q}, matched_rows=\n{matched_rows}")

                if not matched_rows.empty:
                    subject = matched_rows["Môn"].values[0] if "Môn" in matched_rows.columns else ""
                    topic = matched_rows["Chủ đề"].values[0] if "Chủ đề" in matched_rows.columns else ""
                    chapter = matched_rows["Chương"].values[0] if "Chương" in matched_rows.columns else ""
                    lesson = matched_rows["Bài"].values[0] if "Bài" in matched_rows.columns else ""
                    link = matched_rows["Link bài luyện"].values[0] if "Link bài luyện" in matched_rows.columns else ""
                    detail = matched_rows["Chi tiết"].values[0] if "Chi tiết" in matched_rows.columns else ""
                    
                    if isinstance(subject, float):
                        subject = "" if pd.isna(subject) else str(subject)
                    if isinstance(topic, float):
                        topic = "" if pd.isna(topic) else str(topic)
                    if isinstance(chapter, float):
                        chapter = "" if pd.isna(chapter) else str(chapter)
                    if isinstance(lesson, float):
                        lesson = "" if pd.isna(lesson) else str(lesson)
                    if isinstance(link, float):
                        link = "" if pd.isna(link) else str(link)
                    if isinstance(detail, float):
                        detail = "" if pd.isna(detail) else str(detail)

                    # Nhóm theo cấu trúc: Môn → Chủ đề → Chương → Bài (với link)
                    if subject and topic and chapter and lesson:
                        # DEBUG: Case 1 - Full structure
                        if subject not in grouped_dict:
                            grouped_dict[subject] = {}
                        if topic not in grouped_dict[subject]:
                            grouped_dict[subject][topic] = {}
                        if chapter not in grouped_dict[subject][topic]:
                            grouped_dict[subject][topic][chapter] = {}
                        grouped_dict[subject][topic][chapter][lesson] = link if link else ""
                    elif subject and topic and chapter:
                        # DEBUG: Case 2 - No lesson
                        if subject not in grouped_dict:
                            grouped_dict[subject] = {}
                        if topic not in grouped_dict[subject]:
                            grouped_dict[subject][topic] = {}
                        grouped_dict[subject][topic][chapter] = link if link else ""
                    elif topic and chapter and lesson:
                        # DEBUG: Case 3 - No subject
                        if topic not in grouped_dict:
                            grouped_dict[topic] = {}
                        if chapter not in grouped_dict[topic]:
                            grouped_dict[topic][chapter] = {}
                        grouped_dict[topic][chapter][lesson] = link if link else ""
                    elif topic and chapter:
                        # DEBUG: Case 4 - Topic and chapter only
                        if topic not in grouped_dict:
                            grouped_dict[topic] = {}
                        grouped_dict[topic][chapter] = link if link else ""
                    elif topic and lesson:
                        # DEBUG: Case 5 - Topic and lesson only
                        if topic not in grouped_dict:
                            grouped_dict[topic] = {}
                        grouped_dict[topic][lesson] = link if link else ""
                    else:
                        pass
                else:
                    pass

        # Format kết quả với cấu trúc đầy đủ
        formatted_parts = []

        if "Môn" in df2.columns:
            for subject, topics in grouped_dict.items():
                for topic, chapters in topics.items():
                    if isinstance(chapters, dict):  # Có chương
                        for chapter, lessons in chapters.items():
                            if isinstance(lessons, dict):  # Bài học với link
                                lesson_list = []
                                for lesson, link in lessons.items():
                                    if link:
                                        lesson_list.append(f"{lesson} ({link})")
                                    else:
                                        lesson_list.append(lesson)
                                formatted_parts.append(f"Môn {subject} - Chủ đề {topic} - Chương {chapter}: {' - '.join(sorted(lesson_list))}")
                            else:  # Trường hợp không có bài
                                formatted_parts.append(f"Môn {subject} - Chủ đề {topic} - Chương {chapter}")
                    else:  # Không có chương
                        formatted_parts.append(f"Môn {subject} - Chủ đề {topic}: {' - '.join(sorted(chapters))}")
        else:
            for topic, chapters in grouped_dict.items():
                if isinstance(chapters, dict):
                    for chapter, lessons in chapters.items():
                        if isinstance(lessons, dict):
                            lesson_list = []
                            for lesson, link in lessons.items():
                                if link:
                                    lesson_list.append(f"{lesson} ({link})")
                                else:
                                    lesson_list.append(lesson)
                            formatted_parts.append(f"Chủ đề {topic} - Chương {chapter}: {' - '.join(sorted(lesson_list))}")
                        else:
                            # formatted_parts.append(f"Chủ đề {topic}: {' - '.join(sorted(lesson_list))}")
                            lesson_text = f"{chapter} ({lessons})" if lessons else chapter
                            formatted_parts.append(f"Chủ đề {topic}: {lesson_text}")

        formatted_content = "; ".join(formatted_parts) if formatted_parts else ""
        # formatted_content = "; ".join([f"{topic}: {' - '.join(sorted(lessons))}" for topic, lessons in topic_dict.items()])

        improvement_content.append(formatted_content if formatted_content else "")
    
    if len(df1.columns) >= 18:
        column_indices = list(range(18))  # Columns A through R (0-17)
        new_df = df1.iloc[:, column_indices].copy()
    else:
        new_df = df1.copy()

    # Add the new column to the dataframe
    new_df["Đúng"] = correct_list
    new_df["Sai"] = wrong_list
    new_df["Các câu sai"] = wrong_questions_string_list
    new_df["Bỏ qua"] = skip_list
    new_df["Tổng số câu"] = len(result_indices)

    new_df["Mức độ kiến thức cơ bản đạt được"] = basic_percent_list
    new_df["Mức độ kiến thức nâng cao đạt được"] = advanced_percent_list

    # Handle class ranking
    # if "Lớp" in new_df.columns:
    #     # Rank within class
    #     new_df["Thứ hạng trong lớp_rank"] = new_df.groupby("Lớp")["Điểm"].rank(ascending=False, method="min").astype("Int64")
    #     new_df["Thứ hạng trong lớp"] = new_df["Thứ hạng trong lớp_rank"].astype(str) + "/" + new_df.groupby("Lớp")["Điểm"].transform("count").astype(str)

    #     # Extract grade level and handle grade level ranking
    #     new_df["Thứ hạng trong khối_rank"] = new_df.groupby(new_df["Lớp"].str.extract(r'(\d+)')[0])["Điểm"].rank(ascending=False, method="min").astype("Int64")
    #     new_df["Thứ hạng trong khối"] = new_df["Thứ hạng trong khối_rank"].astype(str) + "/" + new_df.groupby(new_df["Lớp"].str.extract(r'(\d+)')[0])["Điểm"].transform("count").astype(str)

    #     # Drop temporary columns
    #     new_df = new_df.drop(columns=["Thứ hạng trong lớp_rank", "Thứ hạng trong khối_rank"])

    new_df["Nội dung cần cải thiện"] = improvement_content

    return new_df

def generate_feedback_sync(args):
    """
    Hàm tạo nhận xét đồng bộ với logic quản lý API key thông minh + Service Account
    """
    (student_name, class_name, point, correct, wrong, skip, total_questions, correct_basic, correct_advanced,
     percent_basic, percent_advanced, improvement_content) = args
    
    # Validate input data trước khi xử lý
    if not student_name or pd.isna(student_name) or str(student_name).strip().lower() in ['', 'nan', 'null', 'none']:
        fallback_name = f"Học sinh (dòng không xác định)"
        logger.warning(f"[Tiến trình {os.getpid()}] Tên học sinh không hợp lệ: {student_name}, sử dụng fallback")
        student_name = fallback_name
    else:
        student_name = str(student_name).strip()
    
    max_attempts = 100  # Tăng số lần thử với Service Account backup
    
    logger.info(f"[Tiến trình {os.getpid()}] Bắt đầu xử lý {student_name}...")
    
    for attempt in range(max_attempts):
        # Kiểm tra xem tất cả keys có bị exhausted không
        if key_manager.check_all_keys_exhausted():
            logger.error(f"🛑 Tất cả API keys và Service Account đã bị rate limit quá 10 lần liên tiếp. Dừng quá trình.")
            break
            
        # Lấy key/credential khả dụng
        available_auth = key_manager.get_available_key()
        if not available_auth:
            logger.warning(f"⏳ [Tiến trình {os.getpid()}] Không có auth khả dụng cho {student_name}, đợi 10s...")
            time.sleep(10)
            continue
        
        auth_type, auth_value = available_auth
        
        try:
            if auth_type == "api_key":
                logger.info(f"[Tiến trình {os.getpid()}] Thử lần {attempt + 1} - {student_name} với API key {auth_value[:20]}...")
                genai.configure(api_key=auth_value)
            elif auth_type == "service_account":
                logger.info(f"[Tiến trình {os.getpid()}] Thử lần {attempt + 1} - {student_name} với Service Account...")
                # Đảm bảo Service Account credentials hoạt động trong child process
                service_account_configured = False
                
                # Thử sử dụng credentials từ manager trước
                if auth_value:
                    try:
                        genai.configure(credentials=auth_value)
                        service_account_configured = True
                        logger.debug(f"[Tiến trình {os.getpid()}] Sử dụng Service Account credentials từ manager")
                    except Exception as cred_error:
                        logger.warning(f"⚠️ [Tiến trình {os.getpid()}] Credentials từ manager bị lỗi: {cred_error}")
                
                # Nếu không được, tạo mới từ env vars (đảm bảo hoạt động trong multiprocessing)
                if not service_account_configured:
                    logger.info(f"🔄 [Tiến trình {os.getpid()}] Tạo lại Service Account credentials từ env vars...")
                    fresh_credentials = create_service_account_credentials()
                    
                    if fresh_credentials:
                        try:
                            genai.configure(credentials=fresh_credentials)
                            service_account_configured = True
                            logger.info(f"✅ [Tiến trình {os.getpid()}] Đã tạo lại Service Account credentials thành công")
                        except Exception as config_error:
                            logger.error(f"❌ [Tiến trình {os.getpid()}] Lỗi configure fresh credentials: {config_error}")
                    
                    if not service_account_configured:
                        logger.error(f"❌ [Tiến trình {os.getpid()}] Không thể configure Service Account, bỏ qua lần thử này")
                        continue
            
            model = genai.GenerativeModel('gemini-2.0-flash')

            prompt = load_prompt(
                student_name=student_name,
                point=f"{point}/135",
                correct=correct,
                wrong=wrong,
                skip=skip,
                total_questions=total_questions,
                correct_basic=correct_basic,
                percent_basic=percent_basic,
                correct_advanced=correct_advanced,
                percent_advanced=percent_advanced,
                improvement_content=improvement_content,
            )

            response = model.generate_content(prompt)
            response.resolve()
            gemini_comment = response.text

            logger.info(f"✅ [Tiến trình {os.getpid()}] Thành công cho {student_name} bằng {auth_type}")
            
            # Log thống kê keys
            stats = key_manager.get_stats()
            logger.info(f"📊 Auth stats: {stats}")
            
            return (student_name, gemini_comment)

        except Exception as e:
            error_msg = str(e)
            error_lower = error_msg.lower()
            
            # Xử lý lỗi 400 - API key expired/invalid (chỉ áp dụng cho API keys)
            if auth_type == "api_key" and "400" in error_msg and ("api key" in error_lower and ("expired" in error_lower or "invalid" in error_lower)):
                logger.error(f"🚫 [Tiến trình {os.getpid()}] API key invalid/expired cho {student_name}: {auth_value[:20]}...")
                key_manager.mark_key_invalid(auth_value)
                continue
                
            # Xử lý rate limit
            elif "quota" in error_lower or "rate" in error_lower or "429" in error_msg:
                logger.warning(f"⚠️ [Tiến trình {os.getpid()}] Rate limit ({auth_type}) cho {student_name}: {str(e)[:100]}...")
                
                # Trích xuất retry_delay
                retry_delay = extract_retry_delay(error_msg)
                key_manager.mark_key_rate_limited(auth_type, auth_value, retry_delay)
                continue
                
            # Lỗi khác
            else:
                logger.error(f"❌ [Tiến trình {os.getpid()}] Lỗi khác ({auth_type}) cho {student_name}: {str(e)[:100]}...")
                time.sleep(2)  # Đợi ngắn cho lỗi khác
                continue

    # Fallback comment nếu không thể tạo được nhận xét
    fallback_comment = (
        f"{student_name} đã đạt {point} điểm trong bài kiểm tra. "
        f"Ở phần kiến thức cơ bản, thí sinh làm đúng {correct_basic} câu ({percent_basic}%), "
        f"còn ở phần nâng cao thí sinh đạt {correct_advanced} câu ({percent_advanced}%). "
        f"Chúng tôi khích lệ thí sinh tiếp tục giữ vững tinh thần học tập và cố gắng tiến bộ hơn trong thời gian tới."
    )

    logger.warning(f"⚠️ [Tiến trình {os.getpid()}] Dùng nhận xét dự phòng cho {student_name} sau {max_attempts} lần thử.")
    return (student_name, fallback_comment)

async def generate_feedback_async(student_name, class_name, point, correct_basic, correct_advanced, percent_basic, percent_advanced, class_rank, grade_rank, improvement_content, semaphore):
    async with semaphore:
        prompt = load_prompt(
            student_name=student_name,
            point=point,
            correct_basic=correct_basic,
            percent_basic=percent_basic,
            correct_advanced=correct_advanced,
            percent_advanced=percent_advanced,
            class_rank=class_rank,
            grade_rank=grade_rank,
            improvement_content=improvement_content,
        )

        max_retries = 3  # Số lần thử tối đa với mỗi AI
        gemini_api_keys = API_KEYS  # Sử dụng danh sách API keys đã load

        for api_index, api_key in enumerate(gemini_api_keys):
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-2.0-flash')

            for attempt in range(max_retries):
                try:
                    logger.info(f"[Thử {attempt + 1}/{max_retries}] Gọi AI {api_index + 1} (Gemini) cho {student_name}...")

                    response = model.generate_content(prompt)
                    response.resolve()  # Kích hoạt tạo nội dung
                    gemini_comment = response.text

                    logger.info(f"✅ Đã tạo nhận xét thành công từ AI {api_index + 1} cho {student_name} - lớp {class_name}")
                    return gemini_comment  # ✅ Thành công, thoát vòng lặp

                except Exception as e:
                    logger.error(f"❌ Lỗi khi gọi AI {api_index + 1} (lần {attempt + 1}/{max_retries}) cho {student_name}")

                    if attempt < max_retries - 1:
                        delay = 5 * (attempt + 1)
                        logger.warning(f"⏳ Đợi {delay} giây trước khi thử lại AI {api_index + 1}...")
                        await asyncio.sleep(delay)

        fallback_comment = (
            f"{student_name} đạt {point} điểm. "
            f"Em đã trả lời đúng {correct_basic} câu cơ bản ({percent_basic}) và {correct_advanced} câu nâng cao ({percent_advanced}). "
            f"Những nội dung cần cải thiện: {improvement_content if improvement_content else 'Không có thông tin cụ thể.'}."
        )

        logger.warning(f"⚠️ Dùng nhận xét dự phòng cho {student_name}.")
        return fallback_comment  # ✅ Đảm bảo luôn có nhận xét

def process_feedbacks_multiprocessing(new_df):
    """
    Xử lý tạo nhận xét với hệ thống quản lý API key thông minh + Service Account
    """
    logger.info("Bắt đầu tạo nhận xét cho học sinh bằng multiprocessing với hệ thống quản lý API key + Service Account...")
    
    if "Nhận xét" not in new_df.columns:
        new_df["Nhận xét"] = ""

    tasks = []
    skipped_students = 0
    
    for index, row in new_df.iterrows():
        student_name = row["Họ và tên"] if "Họ và tên" in row else row["Tên hiển thị"]
        
        # Validate student name - bỏ qua các dòng có tên không hợp lệ
        if pd.isna(student_name) or str(student_name).strip().lower() in ['', 'nan', 'null', 'none']:
            skipped_students += 1
            logger.warning(f"⚠️ Bỏ qua học sinh ở dòng {index + 1}: tên không hợp lệ ({student_name})")
            continue
            
        # Clean student name
        student_name = str(student_name).strip()
        
        class_name = row["Lớp"]
        point = row["Điểm"]
        correct = row["Đúng"]
        wrong = row["Sai"]
        skip = row["Bỏ qua"]
        total_questions = row["Tổng số câu"]
        correct_basic = row["Mức độ kiến thức cơ bản đạt được"]
        correct_advanced = row["Mức độ kiến thức nâng cao đạt được"]
        improvement_content = row["Nội dung cần cải thiện"]
        
        # Validate other required fields
        if pd.isna(point) or pd.isna(correct) or pd.isna(wrong):
            skipped_students += 1
            logger.warning(f"⚠️ Bỏ qua học sinh {student_name}: thiếu dữ liệu điểm số")
            continue
        
        try:
            percent_basic = round(float(correct_basic.replace("%", ""))) if isinstance(correct_basic, str) else round(correct_basic)
            percent_advanced = round(float(correct_advanced.replace("%", ""))) if isinstance(correct_advanced, str) else round(correct_advanced)
        except (ValueError, AttributeError):
            percent_basic = 0
            percent_advanced = 0
        
        task_args = (
            student_name, class_name, point, correct, wrong, skip, total_questions, correct_basic, correct_advanced,
            percent_basic, percent_advanced, improvement_content
        )
        tasks.append(task_args)
    
    if skipped_students > 0:
        logger.info(f"📝 Đã bỏ qua {skipped_students} học sinh có dữ liệu không hợp lệ")
    
    # Sử dụng tối đa số tiến trình mà CPU đang có (tối ưu với số lượng API keys + Service Account)
    cpu_count = mp.cpu_count()
    total_auth_methods = len(API_KEYS) + (1 if service_account_credentials else 0)
    num_processes = min(total_auth_methods // 2, cpu_count // 2, 12)  # Tối đa 12 tiến trình
    logger.info(f"Sử dụng {num_processes} tiến trình (CPU có {cpu_count} cores) với {len(API_KEYS)} API keys + {'Service Account' if service_account_credentials else 'không có SA'} để xử lý {len(tasks)} học sinh...")
    
    # Log thống kê ban đầu
    stats = key_manager.get_stats()
    logger.info(f"📊 Trạng thái auth methods ban đầu: {stats}")
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        future_to_index = {executor.submit(generate_feedback_sync, task): i for i, task in enumerate(tasks)}
        
        completed = 0
        failed = 0
        start_time = time.time()
        
        for future in future_to_index:
            try:
                # Kiểm tra timeout tổng thể (tối đa 2 giờ)
                elapsed_time = time.time() - start_time
                if elapsed_time > 7200:  # 2 giờ
                    logger.error("🕐 Timeout 2 giờ đã đạt. Dừng quá trình để tránh treo hệ thống.")
                    # Cancel remaining futures
                    for remaining_future in future_to_index:
                        if not remaining_future.done():
                            remaining_future.cancel()
                    break
                
                # Kiểm tra xem có nên dừng quá trình không
                if key_manager.check_all_keys_exhausted():
                    logger.error("🛑 Tất cả auth methods đã bị exhausted. Dừng quá trình tạo nhận xét.")
                    # Cancel remaining futures
                    for remaining_future in future_to_index:
                        if not remaining_future.done():
                            remaining_future.cancel()
                    break
                    
                student_name, feedback = future.result(timeout=300)  # Giảm timeout xuống 5 phút để tránh treo
                index = future_to_index[future]
                new_df.at[index, "Nhận xét"] = feedback
                completed += 1
                
                if completed % 5 == 0:  # Log thường xuyên hơn
                    stats = key_manager.get_stats()
                    elapsed = time.time() - start_time
                    rate = completed / elapsed * 60 if elapsed > 0 else 0
                    logger.info(f"Đã hoàn thành {completed}/{len(tasks)} nhận xét ({rate:.1f}/phút)... Auth: {stats}")
                    
            except Exception as e:
                index = future_to_index[future]
                student_name = tasks[index][0] if len(tasks[index]) > 0 else "Không xác định"
                failed += 1
                
                # Xử lý timeout riêng
                if "timeout" in str(e).lower() or "TimeoutError" in str(type(e)):
                    logger.error(f"⏰ Timeout khi xử lý {student_name}: {str(e)[:100]}")
                else:
                    logger.error(f"❌ Lỗi khi xử lý nhận xét cho {student_name}: {str(e)[:200]}")
                
                fallback_comment = (
                    f"{student_name} đạt {tasks[index][2]} điểm trong bài kiểm tra. "
                    f"Thí sinh cần tiếp tục cố gắng để đạt kết quả tốt hơn trong các kỳ thi sắp tới."
                )
                new_df.at[index, "Nhận xét"] = fallback_comment

    # Thống kê cuối cùng
    total_time = time.time() - start_time
    final_stats = key_manager.get_stats()
    
    logger.info(f"✅ Hoàn thành quá trình tạo nhận xét trong {total_time:.1f}s:")
    logger.info(f"   📊 {completed} thành công, {failed} thất bại ({len(tasks)} total)")
    logger.info(f"   ⚡ Tốc độ trung bình: {completed/total_time*60:.1f} nhận xét/phút")
    logger.info(f"   🔑 Auth methods cuối: {final_stats}")
    
    # Kiểm tra nếu có quá nhiều failures
    if failed > completed * 0.5:  # Hơn 50% thất bại
        logger.warning(f"⚠️ Tỷ lệ thất bại cao ({failed}/{len(tasks)}). Kiểm tra lại API keys hoặc Service Account")
    
    return new_df

async def process_feedbacks(new_df):
    semaphore = asyncio.Semaphore(15)  # Giới hạn 15 request cùng lúc
    tasks = []
    
    logger.info("Bắt đầu tạo nhận xét cho học sinh...")

    for index, row in new_df.iterrows():
        student_name = row["Họ và tên"] if "Họ và tên" in row else row["Tên hiển thị"]
        class_name = row["Lớp"]
        point = row["Điểm"]
        correct_basic = row["Mức độ kiến thức cơ bản đạt được"]
        correct_advanced = row["Mức độ kiến thức nâng cao đạt được"]
        class_rank = row["Thứ hạng trong lớp"]
        grade_rank = row["Thứ hạng trong khối"]
        
        # Chuyển đổi kiểu dữ liệu trước khi làm tròn
        try:
            percent_basic = round(float(correct_basic.replace("%", ""))) if isinstance(correct_basic, str) else round(correct_basic)
            percent_advanced = round(float(correct_advanced.replace("%", ""))) if isinstance(correct_advanced, str) else round(correct_advanced)
        except ValueError as ve:
            logger.error(f"Lỗi chuyển đổi dữ liệu cho {student_name}: {ve}")
            percent_basic = 0
            percent_advanced = 0

        improvement_content = row["Nội dung cần cải thiện"]

        tasks.append(generate_feedback_async(student_name, class_name, point, correct_basic, correct_advanced, percent_basic, percent_advanced, class_rank, grade_rank, improvement_content, semaphore))

        if len(tasks) % 15 == 0:
            logger.info("Gửi 15 request, chờ xử lý...")
            results = await asyncio.gather(*tasks)
            tasks.clear()

            for i, feedback in enumerate(results):
                new_df.at[index - 14 + i, "Nhận xét"] = feedback

            logger.info("Đợi 20 giây để tránh bị rate limit...")
            time.sleep(20)

    if tasks:
        logger.info(f"Gửi {len(tasks)} request cuối...")
        results = await asyncio.gather(*tasks)
        for i, feedback in enumerate(results):
            new_df.at[len(new_df) - len(results) + i, "Nhận xét"] = feedback

    logger.info("Hoàn thành quá trình tạo nhận xét.")
    return new_df

def process_sheet_with_multiprocessing(df1, df2):
    """
    Wrapper function để xử lý sheet với multiprocessing và hệ thống quản lý API key + Service Account thông minh
    """
    logger.info("🚀 Bắt đầu xử lý sheet với hệ thống quản lý API key + Service Account thông minh")
    
    # Reset key manager để đảm bảo trạng thái sạch
    reset_key_manager()
    
    # Xử lý dữ liệu trước
    processed_df = handle_sheet(df1, df2)
    logger.info(f"📊 Đã xử lý dữ liệu cho {len(processed_df)} học sinh")
    
    # Tạo nhận xét bằng multiprocessing
    final_df = process_feedbacks_multiprocessing(processed_df)
    
    # Thống kê cuối cùng
    final_stats = key_manager.get_stats()
    logger.info(f"🏁 Hoàn thành xử lý sheet. Thống kê auth methods: {final_stats}")
    
    return final_df

def process_sheet_with_async(df1, df2):
    """
    Wrapper function để xử lý sheet với async (phương pháp cũ)
    """
    # Xử lý dữ liệu trước
    processed_df = handle_sheet(df1, df2)
    
    # Tạo nhận xét bằng async
    final_df = asyncio.run(process_feedbacks(processed_df))
    
    return final_df

if __name__ == "__main__":
    # Thiết lập multiprocessing cho Windows
    mp.set_start_method('spawn', force=True)
    
    # Test với dữ liệu mẫu (thay đổi đường dẫn theo thực tế)
    print("Testing multiprocessing feedback generation...")
    
    # Bạn có thể thêm code test ở đây nếu cần