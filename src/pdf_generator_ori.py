import os
import re
from fpdf import FPDF
from collections import Counter, defaultdict

FONT_DIR = os.path.join(os.path.dirname(__file__), "../assets/fonts/")
IMAGE_DIR = os.path.join(os.path.dirname(__file__), "../assets/images/")
footer_image = os.path.join(IMAGE_DIR, "footer-image.png")
header_image = os.path.join(IMAGE_DIR, "header-image.png")

class PDF(FPDF):
    def footer(self):
        self.set_y(-15) 
        self.image(footer_image, x=0, y=282, w=215)  

def pdf_generator(row, output_pdf):
    pdf = FPDF()
    pdf.add_page()

    pdf.add_font("DejaVu", "", os.path.join(FONT_DIR, "DejaVuSansCondensed.ttf"), uni=True)
    pdf.add_font("DejaVu", "B", os.path.join(FONT_DIR, "DejaVuSansCondensed-Bold.ttf"), uni=True)
    pdf.add_font("DejaVu", "I", os.path.join(FONT_DIR, "DejaVuSansCondensed-Oblique.ttf"), uni=True)
    pdf.add_font("DejaVu", "BI", os.path.join(FONT_DIR, "DejaVuSansCondensed-BoldOblique.ttf"), uni=True)

    pdf.image(header_image , x=0, y=0, w=210)
    pdf.ln(22)

    id = str(row.get("SBD", "Unknown"))
    pdf_file = os.path.join(output_pdf, f"{id}.pdf")

    pdf.set_font("DejaVu", "B", size=13)
    pdf.cell(200, 10, "THÔNG BÁO", ln=True, align="C")
    pdf.cell(200, 10, "KẾT QUẢ KỲ THI TOÁN HỌC HOA KỲ AMC8 – 2025", ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("DejaVu", size=11)
    pdf.cell(0, 8, f"Họ và tên thí sinh: {row['Họ và tên đệm']} {row['Tên']}", ln=True)
    pdf.cell(0, 8, f"Lớp: {row['Lớp']}", ln=True)
    pdf.cell(0, 8, f"Trường: {row['Trường']}", ln=True)
    pdf.cell(0, 8, "Tham gia Kỳ thi Toán học Hoa Kỳ AMC8, ngày thi 22/01/2025.", ln=True)
    pdf.set_font("DejaVu", style="BI")
    pdf.cell(0, 10, "BTC Kỳ thi gửi thông báo kết quả thi như sau:", ln=True, link="")
    pdf.cell(5)
    pdf.set_font("DejaVu", size=11)
    pdf.cell(0, 10, "• Tổng số câu hỏi: 25 câu", ln=True)
    pdf.cell(5)
    student_answers = f"• Thí sinh đã trả lời lần lượt là: {row['Câu trả lời']}"
    pdf.cell(pdf.get_string_width(student_answers) + 2, 10, student_answers, ln=False)
    pdf.set_text_color(0, 0, 255)
    pdf.cell(0, 10, "(Xem lại đề thi tại đây)", link="https://drive.google.com/file/d/1FjNquIMwWxjjupX3VXaUF0gg4yKQ08bm/view", ln=True)
    pdf.set_text_color(0, 0, 0) 
    pdf.cell(5)

    percent_correct = round((row['Số câu trả lời đúng'] / 25) * 100)
    percent_wrong = round((row['Số câu trả lời sai'] / 25) * 100)
    pdf.cell(0, 10, f"• Số câu trả lời đúng: {row['Số câu trả lời đúng']} ({percent_correct}%)", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Số câu trả lời sai: {row['Số câu trả lời sai']} ({percent_wrong}%)", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Điểm số: {row['Số câu trả lời đúng']}/25", ln=True)
    pdf.set_font("DejaVu", style="B")
    pdf.cell(0, 10, "1. Nhận xét về kết quả bài thi:", ln=True)

    pdf.set_font("DejaVu", size=11)
    if isinstance(row['Nhận xét về kết quả bài thi'], str) and row['Nhận xét về kết quả bài thi'].strip():
        list_exercises_correct = row['Nhận xét về kết quả bài thi'].split(";")

        topics = []
        for exercises_correct in list_exercises_correct:
            exercises_correct = exercises_correct.strip()
            if exercises_correct:
                match = re.match(r"(.+) - (.+): (.+)", exercises_correct)
                if match:
                    topic = match.group(1) 
                    topics.append(topic)

        topic_counts = Counter(topics)
        top_topics = [t[0] for t in topic_counts.most_common(5)]

        # Tạo nội dung in ra PDF
        if top_topics:
            topics_text = ", ".join(top_topics)
            pdf.multi_cell(0, 8, f"Thí sinh đã hiểu và áp dụng tốt vào giải các chủ đề: {topics_text}.", ln=True)

    pdf.cell(5)
    pdf.cell(0, 8, f"• Mức độ kiến thức cơ bản đạt được: {row['Mức độ kiến thức cơ bản đạt được']}", ln=True)
    pdf.cell(5)
    pdf.cell(0, 8, f"• Mức độ kiến thức nâng cao đạt được: {row['Mức độ kiến thức nâng cao đạt được']}", ln=True)
    pdf.set_font("DejaVu", style="B")
    pdf.cell(0, 10, "2. Định hướng cải thiện và phát triển:", ln=True)
    pdf.set_font("DejaVu", size=11)
    results_pass = str(row.get("Học sinh trên 20 điểm", "")).strip().lower()
    if results_pass == "x":
        pdf.multi_cell(0, 8, "Thí sinh đã đáp ứng được các kỹ năng và kiến thức môn Toán cần thiết ở lứa tuổi này, BTC hy vọng thí sinh tiếp tục giữ vững và phát huy.", ln=True)
        pdf.ln(1.6)
    else:
        if isinstance(row['Nội dung câu trả lời sai'], str) and row['Nội dung câu trả lời sai'].strip():
            pdf.cell(0, 8, f"Thí sinh tham khảo gợi ý luyện tập theo các kiến thức liên quan như sau:", ln=True)

            list_exercises_wrong = row['Nội dung câu trả lời sai'].split(";")

            # Dictionary để nhóm bài tập theo topic
            topic_dict = defaultdict(list)

            for exercises_wrong in list_exercises_wrong:
                exercises_wrong = exercises_wrong.strip()
                if exercises_wrong:
                    match = re.match(r"(.+?) - (.+?): (.+?)\s*\((https?://[^\)]+)\)", exercises_wrong)
                    if match:
                        topic, content, exercise, link = match.groups()
                    else:
                        match = re.match(r"(.+?) - (.+?): (.+)", exercises_wrong)
                        if match:
                            topic, content, exercise = match.groups()
                            link = None
                        else:
                            continue

                    # Thêm exercise vào topic tương ứng
                    topic_dict[topic].append((exercise, link))

            # Chọn đúng 3 topic có nhiều bài tập nhất
            top_topics = sorted(topic_dict.keys(), key=lambda x: len(topic_dict[x]), reverse=True)[:3]

            total_exercises = 0

            for topic in top_topics:
                if total_exercises >= 5:  
                    break

                pdf.cell(5)
                pdf.cell(0, 8, f"• {topic}:", ln=True)

                exercises_to_print = topic_dict[topic][:2]

                for exercise, link in exercises_to_print:
                    if total_exercises >= 5:
                        break

                    pdf.cell(8)
                    text = f"◦ {exercise}"

                    if link:
                        pdf.cell(pdf.get_string_width(text) + 2, 8, text, ln=False)
                        pdf.set_text_color(0, 0, 255)
                        pdf.cell(0, 8, "(Link bài luyện)", link=link, ln=True)
                        pdf.set_text_color(0, 0, 0)
                    else:
                        pdf.cell(0, 8, text, ln=True)

                    total_exercises += 1

        pdf.add_page()
        pdf.multi_cell(0, 8, "Dựa trên các nội dung định hướng trên, thí sinh có thể truy cập đường liên kết được gợi ý đi kèm mỗi phần kiến thức và đăng nhập tài khoản do Ban Tổ chức cấp để luyện tập (thời gian sử dụng miễn phí đến hết ngày 15/05/2025)", ln=True)
        pdf.ln(1.5)
        pdf.cell(0, 10, f"Tên đăng nhập: {row.get('Tên đăng nhập', 'N/A')}", ln=True)
        pdf.cell(0, 10, f"Mật khẩu: {row.get('Mật khẩu', 'N/A')}", ln=True)

    pdf.multi_cell(0, 8, f"Chúng tôi tin rằng với sự cố gắng và nỗ lực, thí sinh {row['Họ và tên đệm']} {row['Tên']} sẽ tiếp tục đạt được nhiều thành tích cao hơn nữa.", ln=True)
    pdf.ln(1.6)
    pdf.multi_cell(0, 8, "Cảm ơn sự quan tâm và hy vọng chúng tôi sẽ tiếp tục nhận được sự ủng hộ và đồng hành của Quý thầy cô, Quý Phụ huynh và thí sinh trong những kỳ thi tiếp theo.", ln=True)
    pdf.ln(3)
    pdf.set_font("DejaVu", style="B")
    pdf.cell(0, 10, "BTC KỲ THI TOÁN HỌC HOA KỲ - AMC8", ln=True, align="R")

    pdf.image(footer_image , x=0, y=282, w=215)

    pdf.output(pdf_file)