import os
import time
import google.generativeai as genai
import google.api_core.exceptions
import logging
from dotenv import load_dotenv

load_dotenv()
GOOGLE_API_KEY = os.getenv("MY_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("Vui lòng đặt biến môi trường GOOGLE_API_KEY với API key của bạn.")
genai.configure(api_key=GOOGLE_API_KEY)

# Chọn model Gemini 2.0 Flash
model = genai.GenerativeModel('gemini-2.0-flash')


student_data = {
    "Tài khoản": "chiendd62@haiphong.edu.vn",
    "Họ và tên": "DƯƠNG ĐỨC CHIẾN",
    "Tổng câu hỏi": "40",
    "Đúng": "19",
    "Sai": "21",
    "Bỏ qua": "0",
    "general_result": {
        "score": "4.75",
        "result": "yếu",
        "basic_level": "41,67%",
        "advance_level": "56,25%",
        "rank": "top 33",
        "nội dung cần cải thiện": """
        +) Từ vựng: tìm từ đồng nghĩa - trái nghĩa các bài: Unit 3: Heatthy living for teens; Unit 8: Tourism; Unit 10: Planet Earth
        +) Ngữ pháp
        - Tìm từ đồng nghĩa - trái nghĩa 
        - Mệnh đề nhượng bộ, mệnh đề quan hệ, mệnh đề chỉ thời gian
        - Dạng bài đục lỗ về từ vựng và giới từ
        - Dạng bài đọc hiểu
        - 
        +) Ngữ âm: 
        - Phân biệt âm /u:/ và /ʌ/
        - Tìm trọng âm của từ có 3 âm tiết
        """
    }
}


# prompt = prompt_template.format(**student_data)

def ai_comment_generator(name, correct_answer, incorrect_answers, basic_level, advance_level, wrong_text):

    prompt_template = f"""Write a comment to the student's parents based on the following information:
    Họ và tên: {name}
    Tổng câu hỏi: 25
    Đúng: {correct_answer}
    Sai: {incorrect_answers}
    general_result:
    {{
        score: {correct_answer}/25,
        basic_level: {basic_level},
        advance_level: {advance_level},
        nội dung cần cải thiện: {wrong_text}
    }}

    Please make the comment concise, professional, and encouraging.  Focus on both strengths and areas for improvement.  Suggest specific actions the parents can take to help their child. Keep the message under 150 words.
    Lưu ý: Trả kết quả về tiếng Việt.
    """

    try:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = model.generate_content(prompt_template)
                response.resolve() # Bắt buộc để kích hoạt tạo nội dung
                gemini_comment = response.text
                logging.info(f"Đã tạo nhận xét cho {name}")
                return gemini_comment #Thoát khỏi vòng lặp retry nếu thành công

            except Exception as e:
                logging.error(f"Lỗi khi gọi Gemini API (lần {attempt + 1}/{max_retries}) cho {name}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))  # Tăng thời gian chờ giữa các lần thử
                else:
                    gemini_comment = f"Không thể tạo nhận xét từ Gemini sau {max_retries} lần thử."
                    return gemini_comment #Trả về thông báo lỗi sau khi thử lại
                
    except Exception as e:
        logging.error(f"Lỗi trong quá trình gọi Gemini API: {e}")
        return ""

    return gemini_comment