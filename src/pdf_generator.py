import os
import re
import pandas as pd
from fpdf import FPDF
from collections import Counter, defaultdict

# print(FPDF)

FONT_DIR = os.path.join(os.path.dirname(__file__), "../assets/fonts/")
IMAGE_DIR = os.path.join(os.path.dirname(__file__), "../assets/images/")
footer_image = os.path.join(IMAGE_DIR, "footer-image.png")
header_image = os.path.join(IMAGE_DIR, "header-image.png")

# Đường dẫn file Excel đầu vào
input_excel = "data/output/output.xlsx"
output_folder = "data/output/Tổng hợp báo cáo"

# Tạo thư mục output nếu chưa có
os.makedirs(output_folder, exist_ok=True)

def sanitize_filename(filename):
    """Loại bỏ ký tự đặc biệt để tránh lỗi khi tạo file."""
    return "".join(c if c.isalnum() or c in " _-" else "_" for c in filename)

def check_and_add_page(pdf, lines_count, line_height=8, margin_bottom=15):
    """Kiểm tra nếu không đủ chỗ cho n dòng nữa thì thêm trang"""
    needed_space = lines_count * line_height
    if pdf.get_y() + needed_space + margin_bottom > pdf.h:
        pdf.add_page()

class PDF(FPDF):
    def footer(self):
        self.set_y(-15) 
        self.image(footer_image, x=0, y=282, w=215)  

def pdf_generator(row, sheet_name, output_pdf):
    pdf = FPDF()
    pdf.add_page()

    pdf.add_font("DejaVu", "", os.path.join(FONT_DIR, "DejaVuSansCondensed.ttf"), uni=True)
    pdf.add_font("DejaVu", "B", os.path.join(FONT_DIR, "DejaVuSansCondensed-Bold.ttf"), uni=True)
    pdf.add_font("DejaVu", "I", os.path.join(FONT_DIR, "DejaVuSansCondensed-Oblique.ttf"), uni=True)
    pdf.add_font("DejaVu", "BI", os.path.join(FONT_DIR, "DejaVuSansCondensed-BoldOblique.ttf"), uni=True)

    pdf.image(header_image , x=0, y=0, w=210)
    pdf.ln(22)

    # Lấy thông tin từ dòng dữ liệu
    student_name = sanitize_filename(str(row["Họ và tên"]).strip())
    class_name = sanitize_filename(str(row["Lớp"]).strip())
    match = re.match(r"(.+?)\s+Khối\s+\d+", sheet_name)
    subject_name = match.group(1) if match else sheet_name

    # Đặt tên file PDF theo format: Lớp_Họ và tên_Môn.pdf
    pdf_filename = f"{class_name}_{student_name}_{sheet_name}.pdf"
    pdf_path = os.path.join(output_pdf, pdf_filename)

    percent_correct = round((row['Đúng'] / row['Tổng câu hỏi']) * 100)
    percent_wrong = round((row['Sai'] / row['Tổng câu hỏi']) * 100)

    pdf.set_font("DejaVu", "B", size=13)
    pdf.cell(200, 10, "THÔNG BÁO", ln=True, align="C")
    pdf.cell(200, 10, "KẾT QUẢ THI CUỐI KỲ II NĂM HỌC 2024 - 2025", ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("DejaVu", size=11)
    pdf.cell(0, 8, f"Họ và tên: {row['Họ và tên']}", ln=True)
    pdf.cell(0, 8, f"Lớp: {row['Lớp']}", ln=True)
    pdf.cell(0, 8, f"Môn: {subject_name}", ln=True)
    pdf.set_font("DejaVu", style="BI")
    pdf.cell(0, 10, "Nhà trường gửi thông báo kết quả thi như sau:", ln=True, link="")
    pdf.cell(5)
    pdf.set_font("DejaVu", size=11)
    pdf.cell(0, 10, f"• Tổng số câu hỏi: {row['Tổng câu hỏi']} câu", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Số câu trả lời đúng: {row['Đúng']} ({percent_correct}%)", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Số câu trả lời sai: {row['Sai']} ({percent_wrong}%)", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Điểm số: {row['Điểm']}", ln=True)
    pdf.set_font("DejaVu", style="B")
    pdf.cell(0, 10, "1. Kết quả chi tiết cho thấy:", ln=True)
    pdf.cell(5)
    pdf.set_font("DejaVu", size=11)
    pdf.cell(0, 8, f"• Mức độ kiến thức cơ bản đạt được: {row['Mức độ kiến thức cơ bản đạt được']}", ln=True)
    pdf.cell(5)
    pdf.cell(0, 8, f"• Mức độ kiến thức nâng cao đạt được: {row['Mức độ kiến thức nâng cao đạt được']}", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Xếp hạng trong lớp: {row['Thứ hạng trong lớp']}", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Xếp hạng trong toàn khối: {row['Thứ hạng trong khối']}", ln=True)
    pdf.set_font("DejaVu", style="B")
    pdf.cell(0, 10, "2. Định hướng cải thiện và phát triển:", ln=True)
    pdf.set_font("DejaVu", size=11)

    feedbacks = re.sub(r'\n\s*\n', '\n', str(row['Nhận xét'])).strip()
    pdf.multi_cell(0, 8, feedbacks, ln=True)
    pdf.ln(2)

    if isinstance(row['Nội dung cần cải thiện'], str) and row['Nội dung cần cải thiện'].strip():
        pdf.cell(0, 8, f"Em có thể tham khảo gợi ý luyện tập theo các kiến thức liên quan như sau:", ln=True)

        topics = row['Nội dung cần cải thiện'].split(";")
        topics = [t.strip() for t in topics if t.strip()]

        has_subject = any(re.match(r"(.+?)\s*-\s*(.+?):\s*(.+)", t) for t in topics)

        if has_subject:
            pdf_structure = {}
            topic_counts = {}

            for topic_data in topics:
                match = re.match(r"(.+?)\s*-\s*(.+?):\s*(.+)", topic_data)
                if match:
                    subject, topic, content_list = match.groups()
                    subject = subject.strip()
                    topic = topic.strip()
                    contents = [c.strip() for c in content_list.split(" - ")][:2]  # Tối đa 2 bài

                    # Khởi tạo nếu môn chưa có
                    if subject not in pdf_structure:
                        pdf_structure[subject] = {}
                        topic_counts[subject] = 0

                    # Bỏ qua nếu đã đủ 4 topic
                    if topic_counts[subject] >= 4:
                        continue

                    # Bỏ qua nếu topic đã tồn tại
                    if topic in pdf_structure[subject]:
                        continue

                    # Ghi nhận topic và tăng đếm
                    pdf_structure[subject][topic] = contents
                    topic_counts[subject] += 1

            # In theo thứ tự ưu tiên nếu có, rồi đến các môn còn lại
            subject_priority = ["Toán", "Ngữ Văn", "Tiếng Anh"]
            printed_subjects = set()

            for subject in subject_priority:
                if subject in pdf_structure:
                    pdf.set_font("DejaVu", style="I")
                    pdf.cell(0, 8, f"- {subject}", ln=True)
                    pdf.set_font("DejaVu", size=11)
                    for topic, lessons in pdf_structure[subject].items():
                        lines_needed = 1 + len(lessons)
                        check_and_add_page(pdf, lines_count=lines_needed)

                        pdf.cell(8)
                        pdf.cell(0, 8, f"• {topic}:", ln=True)
                        for lesson in lessons:
                            pdf.cell(16)
                            pdf.cell(0, 8, f"◦ {lesson}", ln=True)
                    printed_subjects.add(subject)

            # In các môn còn lại (ngoài danh sách ưu tiên)
            for subject in pdf_structure:
                if subject not in printed_subjects:
                    pdf.set_font("DejaVu", style="I")
                    pdf.cell(0, 8, f"- {subject}", ln=True)
                    pdf.set_font("DejaVu", size=11)
                    for topic, lessons in pdf_structure[subject].items():
                        lines_needed = 1 + len(lessons)
                        check_and_add_page(pdf, lines_count=lines_needed)

                        pdf.cell(8)
                        pdf.cell(0, 8, f"• {topic}:", ln=True)
                        for lesson in lessons:
                            pdf.cell(16)
                            pdf.cell(0, 8, f"◦ {lesson}", ln=True)

        else:
            for topic_data in topics:
                match = re.match(r"(.+?):\s*(.+)", topic_data)
                if match:
                    topic, content_list = match.groups()
                    topic = topic.strip()
                    contents = [c.strip() for c in content_list.split(" - ")][:3]

                    pdf.cell(5)
                    pdf.cell(0, 8, f"• {topic}:", ln=True)
                    for content in contents:
                        pdf.cell(8)
                        pdf.cell(0, 8, f"◦ {content}", ln=True)

    pdf.ln(3)
    pdf.multi_cell(0, 8, f"Chúng tôi tin rằng với sự cố gắng và nỗ lực, thí sinh {row['Họ và tên']} sẽ tiếp tục đạt được nhiều thành tích cao hơn nữa.", ln=True)
    pdf.ln(1.6)
    pdf.multi_cell(0, 8, "Cảm ơn sự quan tâm và hy vọng chúng tôi sẽ tiếp tục nhận được sự ủng hộ, đồng hành của Quý Phụ huynh và thí sinh trong những kỳ thi tiếp theo.", ln=True)

    pdf.image(footer_image , x=0, y=278, w=215)

    pdf.output(pdf_path)

def process_excel(input_file, output_folder):
    """Hàm xử lý file Excel và tạo các file PDF"""
    # Đọc tất cả các sheet trong file Excel
    xls = pd.ExcelFile(input_file)

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name)

        if "Lớp" not in df.columns or "Họ và tên" not in df.columns:
            print(f"Bỏ qua sheet {sheet_name} do không có cột 'Lớp' hoặc 'Họ và tên'.")
            continue

        # Xử lý từng học sinh
        for _, row in df.iterrows():
            class_name = str(row["Lớp"]).strip()
            sheet_output_folder = os.path.join(output_folder, sheet_name, f"Lớp {class_name}")
            os.makedirs(sheet_output_folder, exist_ok=True)

            # Tạo PDF cho từng học sinh
            pdf_generator(row, sheet_name, sheet_output_folder)


# Gọi hàm xử lý file Excel
if __name__ == "__main__":
    process_excel(input_excel, output_folder)
    print("Hoàn tất tạo file PDF!")