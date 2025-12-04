import os
import time
import pandas as pd
import google.generativeai as genai
from dotenv import load_dotenv
from logger_config import logger
from utils.helpers import load_prompt
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
from datetime import datetime, timedelta
import threading
from collections import defaultdict
import json
from google.oauth2 import service_account

load_dotenv()

class ServiceAccountProcessor:
    """
    Processor chỉ sử dụng Service Account, không dùng API keys
    """
    
    def __init__(self):
        self.service_account_creds = self._create_service_account()
        self.request_count = []
        self.lock = threading.Lock()
        
    def _create_service_account(self):
        """Tạo Service Account credentials từ env vars"""
        try:
            # Thử từ JSON string hoặc file trước
            SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
            SERVICE_ACCOUNT_KEY_JSON = os.getenv("SERVICE_ACCOUNT_KEY_JSON")
            
            if SERVICE_ACCOUNT_KEY_JSON:
                service_account_info = json.loads(SERVICE_ACCOUNT_KEY_JSON)
                creds = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=[
                        'https://www.googleapis.com/auth/cloud-platform',
                        'https://www.googleapis.com/auth/generative-language'
                    ]
                )
                logger.info("✅ Service Account từ JSON string")
                return creds
                
            elif SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
                creds = service_account.Credentials.from_service_account_file(
                    SERVICE_ACCOUNT_FILE,
                    scopes=[
                        'https://www.googleapis.com/auth/cloud-platform',
                        'https://www.googleapis.com/auth/generative-language'
                    ]
                )
                logger.info(f"✅ Service Account từ file: {SERVICE_ACCOUNT_FILE}")
                return creds
                
            else:
                # Tạo từ env vars riêng lẻ
                project_id = os.getenv('PROJECT_ID')
                private_key = os.getenv('PRIVATE_KEY')
                client_email = os.getenv('CLIENT_EMAIL')
                
                if not all([project_id, private_key, client_email]):
                    logger.error("❌ Thiếu thông tin Service Account")
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
                
                creds = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=[
                        'https://www.googleapis.com/auth/cloud-platform',
                        'https://www.googleapis.com/auth/generative-language'
                    ]
                )
                logger.info("✅ Service Account từ environment variables")
                return creds
                
        except Exception as e:
            logger.error(f"❌ Lỗi tạo Service Account: {e}")
            return None
    
    def can_make_request(self):
        """Kiểm tra có thể gửi request không (60 req/min limit)"""
        with self.lock:
            current_time = datetime.now()
            one_minute_ago = current_time - timedelta(minutes=1)
            
            # Cleanup old requests
            self.request_count = [req_time for req_time in self.request_count if req_time > one_minute_ago]
            
            return len(self.request_count) < 60
    
    def record_request(self):
        """Ghi nhận một request"""
        with self.lock:
            self.request_count.append(datetime.now())
    
    def get_stats(self):
        """Lấy thống kê"""
        with self.lock:
            current_time = datetime.now()
            one_minute_ago = current_time - timedelta(minutes=1)
            recent_requests = [req_time for req_time in self.request_count if req_time > one_minute_ago]
            
            return {
                'recent_requests': len(recent_requests),
                'remaining_quota': 60 - len(recent_requests),
                'service_account_available': self.service_account_creds is not None
            }

# Global service account processor
sa_processor = ServiceAccountProcessor()

def generate_feedback_service_account(args):
    """
    Tạo feedback chỉ bằng Service Account
    """
    (student_name, class_name, point, correct, wrong, skip, total_questions, correct_basic, correct_advanced,
     percent_basic, percent_advanced, improvement_content) = args
    
    # Validate input
    if pd.isna(student_name) or not student_name or str(student_name).strip() == "" or str(student_name).lower() == 'nan':
        logger.warning(f"⚠️ [Tiến trình {os.getpid()}] Tên học sinh không hợp lệ: {student_name}, bỏ qua...")
        fallback = f"Học sinh đạt {point} điểm trong bài kiểm tra. Cần tiếp tục cố gắng để đạt kết quả tốt hơn."
        return (str(student_name), fallback)
    
    logger.info(f"[Tiến trình {os.getpid()}] Bắt đầu xử lý {student_name} với Service Account...")
    
    max_attempts = 30
    
    for attempt in range(max_attempts):
        # Kiểm tra rate limit
        if not sa_processor.can_make_request():
            logger.warning(f"⏳ [Tiến trình {os.getpid()}] Service Account rate limit, đợi 10s...")
            time.sleep(10)
            continue
        
        try:
            # Tạo lại Service Account credentials trong child process
            project_id = os.getenv('PROJECT_ID')
            private_key = os.getenv('PRIVATE_KEY')
            client_email = os.getenv('CLIENT_EMAIL')
            
            if not all([project_id, private_key, client_email]):
                logger.error(f"❌ [Tiến trình {os.getpid()}] Thiếu thông tin Service Account")
                break
                
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
            
            credentials = service_account.Credentials.from_service_account_info(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/cloud-platform',
                    'https://www.googleapis.com/auth/generative-language'
                ]
            )
            
            # Configure genai với Service Account
            genai.configure(credentials=credentials)
            model = genai.GenerativeModel('gemini-2.0-flash')

            # Tạo prompt
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

            # Gửi request
            sa_processor.record_request()
            response = model.generate_content(prompt)
            response.resolve()
            gemini_comment = response.text

            logger.info(f"✅ [Tiến trình {os.getpid()}] Thành công cho {student_name} với Service Account")
            return (student_name, gemini_comment)

        except Exception as e:
            error_msg = str(e)
            error_lower = error_msg.lower()
            
            # Xử lý rate limit
            if "quota" in error_lower or "rate" in error_lower or "429" in error_msg:
                logger.warning(f"⚠️ [Tiến trình {os.getpid()}] Service Account rate limit cho {student_name}")
                time.sleep(15)  # Đợi lâu hơn cho Service Account
                continue
                
            # Xử lý lỗi khác
            else:
                logger.error(f"❌ [Tiến trình {os.getpid()}] Lỗi Service Account cho {student_name}: {str(e)[:100]}...")
                time.sleep(5)
                continue

    # Fallback comment
    fallback_comment = (
        f"{student_name} đã đạt {point} điểm trong bài kiểm tra. "
        f"Ở phần kiến thức cơ bản, thí sinh làm đúng {correct_basic} câu ({percent_basic}%), "
        f"còn ở phần nâng cao thí sinh đạt {correct_advanced} câu ({percent_advanced}%). "
        f"Chúng tôi khích lệ thí sinh tiếp tục giữ vững tinh thần học tập và cố gắng tiến bộ hơn trong thời gian tới."
    )

    logger.warning(f"⚠️ [Tiến trình {os.getpid()}] Dùng nhận xét dự phòng cho {student_name} sau {max_attempts} lần thử.")
    return (student_name, fallback_comment)

def process_feedbacks_service_account(new_df):
    """
    Xử lý tạo nhận xét chỉ bằng Service Account
    """
    logger.info("🏢 Bắt đầu tạo nhận xét cho học sinh chỉ bằng Service Account...")
    
    if not sa_processor.service_account_creds:
        logger.error("❌ Không có Service Account credentials!")
        return new_df
    
    if "Nhận xét" not in new_df.columns:
        new_df["Nhận xét"] = ""

    # Filter ra các dòng không hợp lệ
    valid_rows = []
    tasks = []
    
    for index, row in new_df.iterrows():
        student_name = row["Họ và tên"] if "Họ và tên" in row else row.get("Tên hiển thị", "")
        
        # Skip invalid student names
        if pd.isna(student_name) or not student_name or str(student_name).strip() == "" or str(student_name).lower() == 'nan':
            logger.warning(f"⚠️ Bỏ qua học sinh có tên không hợp lệ: {student_name}")
            continue
            
        valid_rows.append(index)
        
        class_name = row["Lớp"]
        point = row["Điểm"]
        correct = row["Đúng"]
        wrong = row["Sai"]
        skip = row["Bỏ qua"]
        total_questions = row["Tổng số câu"]
        correct_basic = row["Mức độ kiến thức cơ bản đạt được"]
        correct_advanced = row["Mức độ kiến thức nâng cao đạt được"]
        improvement_content = row["Nội dung cần cải thiện"]
        
        try:
            percent_basic = round(float(correct_basic.replace("%", ""))) if isinstance(correct_basic, str) else round(correct_basic)
            percent_advanced = round(float(correct_advanced.replace("%", ""))) if isinstance(correct_advanced, str) else round(correct_advanced)
        except ValueError:
            percent_basic = 0
            percent_advanced = 0
        
        task_args = (
            student_name, class_name, point, correct, wrong, skip, total_questions, correct_basic, correct_advanced,
            percent_basic, percent_advanced, improvement_content
        )
        tasks.append((index, task_args))

    logger.info(f"📊 Có {len(tasks)} học sinh hợp lệ để xử lý (bỏ qua {len(new_df) - len(tasks)} dòng không hợp lệ)")
    
    if not tasks:
        logger.error("❌ Không có học sinh hợp lệ nào để xử lý!")
        return new_df
    
    # Sử dụng ít process hơn với Service Account (để tránh rate limit)
    cpu_count = mp.cpu_count()
    num_processes = min(4, cpu_count // 4, len(tasks))  # Tối đa 4 processes để tránh spam Service Account
    num_processes = max(1, num_processes)  # Ít nhất 1 process
    
    logger.info(f"🔧 Sử dụng {num_processes} tiến trình để xử lý {len(tasks)} học sinh với Service Account...")
    
    # Log thống kê ban đầu
    stats = sa_processor.get_stats()
    logger.info(f"📊 Service Account stats: {stats}")
    
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        future_to_data = {executor.submit(generate_feedback_service_account, task_args): (index, task_args) for index, task_args in tasks}
        
        completed = 0
        failed = 0
        
        for future in future_to_data:
            try:
                student_name, feedback = future.result(timeout=300)  # 5 phút timeout
                index, _ = future_to_data[future]
                new_df.at[index, "Nhận xét"] = feedback
                completed += 1
                
                if completed % 5 == 0:  # Log ít hơn để giảm spam
                    stats = sa_processor.get_stats()
                    logger.info(f"📈 Đã hoàn thành {completed}/{len(tasks)} nhận xét... Service Account: {stats}")
                    
            except Exception as e:
                index, task_args = future_to_data[future]
                student_name = task_args[0]
                failed += 1
                logger.error(f"❌ Lỗi khi xử lý nhận xét cho {student_name}: {str(e)}")
                
                # Fallback comment cho lỗi
                fallback_comment = f"{student_name} đạt điểm tốt trong bài kiểm tra. Tiếp tục cố gắng để đạt kết quả tốt hơn."
                new_df.at[index, "Nhận xét"] = fallback_comment

    # Thống kê cuối
    final_stats = sa_processor.get_stats()
    logger.info(f"🎯 Hoàn thành: {completed} thành công, {failed} thất bại")
    logger.info(f"📊 Service Account final stats: {final_stats}")

    return new_df

if __name__ == "__main__":
    # Test function
    logger.info("🧪 Testing Service Account Processor...")
    
    if sa_processor.service_account_creds:
        logger.info("✅ Service Account sẵn sàng")
    else:
        logger.error("❌ Service Account không khả dụng")