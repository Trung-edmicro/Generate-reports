import os
import time
import concurrent.futures
import google.generativeai as genai
import google.api_core.exceptions
from dotenv import load_dotenv

load_dotenv()
GOOGLE_API_KEY = os.getenv("MY_API_KEY")
if not GOOGLE_API_KEY:
    raise ValueError("Vui lòng đặt biến môi trường GOOGLE_API_KEY với API key của bạn.")
genai.configure(api_key=GOOGLE_API_KEY)

# Chọn model Gemini 2.0 Flash
model = genai.GenerativeModel('gemini-2.0-flash')  # Thay đổi nếu có phiên bản khác

# Hàm thực hiện một request
def make_request(prompt):
    try:
        response = model.generate_content(prompt)
        return response.text
    except google.api_core.exceptions.ResourceExhausted as e:
        print(f"Lỗi Rate Limit: {e}")
        return "RATE_LIMITED"
    except Exception as e:
        print(f"Lỗi khi gửi request: {e}")
        return None

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

# Tạo prompt động
prompt_template = """Write a comment to the student's parents based on the following information:
Tài khoản: {Tài khoản}
Họ và tên: {Họ và tên}
Tổng câu hỏi: {Tổng câu hỏi}
Đúng: {Đúng}
Sai: {Sai}
Bỏ qua: {Bỏ qua}
general_result:
{{
    score: {general_result[score]},
    result: {general_result[result]},
    basic_level: {general_result[basic_level]},
    advance_level: {general_result[advance_level]},
    rank: {general_result[rank]},
    nội dung cần cải thiện: {general_result[nội dung cần cải thiện]}
}}

Please make the comment concise, professional, and encouraging.  Focus on both strengths and areas for improvement.  Suggest specific actions the parents can take to help their child. Keep the message under 150 words.
Lưu ý: Trả kết quả về tiếng Việt.
"""

prompt = prompt_template.format(**student_data)
# Số lượng request đồng thời cần thử

num_requests = 15  # Thay đổi số lượng request theo ý muốn

# Đo thời gian và kiểm tra rate limit
def test_rate_limit(num_requests, prompt):
    start_time = time.time()
    success_count = 0
    error_count = 0
    rate_limit_count = 0  # Thêm biến đếm
    successful_responses = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_requests) as executor:
        futures = [executor.submit(make_request, prompt) for _ in range(num_requests)]

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result == "RATE_LIMITED":
                    rate_limit_count += 1
                elif result:
                    success_count += 1
                    successful_responses.append(result)
                else:
                    error_count += 1
            except Exception as e:
                print(f"Lỗi future: {e}")
                error_count += 1

    end_time = time.time()
    duration = end_time - start_time

    print(f"Số request thành công: {success_count}")
    print(f"Số request thất bại: {error_count}")
    print(f"Số lần bị Rate Limit: {rate_limit_count}") # In ra số lần bị rate limit
    print(f"Thời gian thực hiện: {duration:.2f} giây")
    print(f"Số request/giây (ước tính): {success_count / duration:.2f}")

    estimated_wait_time = rate_limit_count * 60  # Ước tính 60 giây chờ cho mỗi lần bị rate limit
    print(f"Thời gian chờ ước tính: {estimated_wait_time:.2f} giây")

    print("\nNội dung các response thành công:")  # In ra nội dung
    for response_text in successful_responses:
        print("-" * 20)  # In một đường phân cách
        print(response_text)

# Chạy thử nghiệm
if __name__ == "__main__":
    test_rate_limit(num_requests, prompt)