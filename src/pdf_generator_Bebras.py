import os
import re
import pandas as pd
from fpdf import FPDF
from collections import Counter, defaultdict

FONT_DIR = os.path.join(os.path.dirname(__file__), "../assets/fonts/")
IMAGE_DIR = os.path.join(os.path.dirname(__file__), "../assets/images/")
header_image = os.path.join(IMAGE_DIR, "header-image_bebras.jpg")
footer_image = os.path.join(IMAGE_DIR, "footer-image_bebras.jpg")

# Đường dẫn file Excel đầu vào
input_excel = "data/output/outputBebras.xlsx"
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
    school_name = sanitize_filename(str(row["Trường"]).strip())
    code_id = sanitize_filename(str(row.get("Mã định danh")).strip())

    match = re.match(r"(.+?)\s+Khối\s+\d+", sheet_name)
    subject_name = match.group(1) if match else sheet_name

    # Đặt tên file PDF
    pdf_filename = f"{class_name}_{student_name}_{code_id}_Thi thử lần 1.pdf"
    pdf_path = os.path.join(output_pdf, pdf_filename)

    total_questions = row['Tổng số câu'] if 'Tổng số câu' in row else (row['Đúng'] + row['Sai'] + row['Bỏ qua']) if (row['Đúng'] + row['Sai'] + row['Bỏ qua']) > 0 else ""

    percent_correct = round((row['Đúng'] / total_questions) * 100)
    percent_wrong = round((row['Sai'] / total_questions) * 100)
    wrong_list = str(row['Các câu sai']) if 'Các câu sai' in row else ""
    percent_skipped = round((row['Bỏ qua'] / total_questions) * 100)

    pdf.set_font("DejaVu", "B", size=13)
    pdf.cell(200, 10, "KỲ THI THÁCH THỨC TƯ DUY THUẬT TOÁN BEBRAS 2026", ln=True, align="C")
    pdf.cell(200, 10, "THÔNG BÁO", ln=True, align="C")
    pdf.cell(200, 10, "KẾT QUẢ BÀI THI THỬ LẦN 1 – NGÀY 21/09/2025", ln=True, align="C")
    pdf.ln(5)
    pdf.set_font("DejaVu", size=11)
    pdf.cell(0, 8, f"Họ và tên: {row['Họ và tên']}", ln=True)
    pdf.cell(0, 8, f"Lớp: {row['Lớp']}", ln=True)
    pdf.cell(0, 8, f"Trường: {row['Trường']}", ln=True)
    pdf.set_font("DejaVu", style="BI")
    pdf.cell(0, 10, "BTC Kỳ thi gửi thông báo kết quả thi như sau: ", ln=True, link="")
    pdf.cell(5)
    pdf.set_font("DejaVu", size=11)
    pdf.cell(0, 10, f"• Tổng số câu hỏi: {total_questions} câu", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Số câu trả lời đúng: {row['Đúng']} ({percent_correct}%)", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Số câu trả lời sai: {row['Sai']} ({percent_wrong}%)", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Số câu trả lời bỏ qua: {row['Bỏ qua']} ({percent_skipped}%)", ln=True)
    pdf.cell(5)
    pdf.multi_cell(0, 10, f"• Các câu sai: {wrong_list}", ln=True)
    pdf.cell(5)
    pdf.cell(0, 10, f"• Điểm số: {row['Điểm']}/135", ln=True)
    pdf.set_text_color(0, 0, 255)
    pdf.cell(5)
    pdf.cell(0, 10, "(Xem lại đề thi tại đây)", link="https://drive.google.com/drive/folders/12n-TOSDSNZC1ERwUJGyfTMIKttmm7d59?usp=sharing", ln=True)
    pdf.set_text_color(0, 0, 0) 
    pdf.set_font("DejaVu", style="B")
    pdf.cell(0, 10, "1. Kết quả chi tiết cho thấy:", ln=True)
    pdf.cell(5)
    pdf.set_font("DejaVu", size=11)
    pdf.cell(0, 8, f"• Mức độ kiến thức cơ bản đạt được: {row['Mức độ kiến thức cơ bản đạt được']}", ln=True)
    pdf.cell(5)
    pdf.cell(0, 8, f"• Mức độ kiến thức nâng cao đạt được: {row['Mức độ kiến thức nâng cao đạt được']}", ln=True)
    # pdf.cell(5)
    # pdf.cell(0, 10, f"• Xếp hạng trong lớp: {row['Thứ hạng trong lớp']}", ln=True)
    # pdf.cell(5)
    # pdf.cell(0, 10, f"• Xếp hạng trong toàn khối: {row['Thứ hạng trong khối']}", ln=True)
    pdf.set_font("DejaVu", style="B")
    pdf.cell(0, 10, "2. Định hướng cải thiện và phát triển:", ln=True)
    pdf.set_font("DejaVu", size=11)
    pdf.set_font("DejaVu", style="I")
    pdf.cell(0, 10, "(Lưu ý các nhận xét dưới đây chỉ mang tính chất tham khảo)", ln=True)
    pdf.set_font("DejaVu", size=11)
    
    if isinstance(row['Nội dung cần cải thiện'], str) and row['Nội dung cần cải thiện'].strip():

        feedbacks = re.sub(r'\n\s*\n', '\n', str(row['Nhận xét'])).strip()
        pdf.multi_cell(0, 8, feedbacks, ln=True)
        pdf.ln(2)
        pdf.cell(0, 8, f"Thí sinh có thể tham khảo luyện tập theo các kiến thức liên quan như sau:", ln=True)

        topics = row['Nội dung cần cải thiện'].split(";")
        topics = [t.strip() for t in topics if t.strip()]

        has_subject = False
        for t in topics:
            match = re.match(r"(.+?)\s*-\s*(.+?):\s*(.+)", t)
            if match:
                subject_candidate = match.group(1).strip()
                if "Chủ đề" not in subject_candidate:
                    has_subject = True
                    break

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
            # Gộp logic nhóm cho phần else với cấu trúc: Chủ đề [topic] - Chương [chapter]: [lesson] (link)
            pdf_structure = {}
            topic_counts = {}

            # Thu thập tất cả lessons để đếm tần suất
            all_lessons = []
            lessons_with_links = {}  # Lưu mapping lesson -> link
            
            for topic_data in topics:
                
                # Regex để parse: Chủ đề [topic]: [lessons] (link) - có link
                match_with_link = re.match(r"Chủ đề\s*(.+?):\s*(.+?)\s*\((https?://.+?)\)", topic_data)
                # Regex để parse: Chủ đề [topic]: [lessons] - không có link  
                match_without_link = re.match(r"Chủ đề\s*(.+?):\s*(.+?)$", topic_data)
                
                if match_with_link:
                    topic, lessons_str, link = match_with_link.groups()
                    topic = topic.strip()
                    lessons_str = lessons_str.strip()
                    
                    # Tách các lesson bằng dấu phẩy
                    lessons = [lesson.strip() for lesson in lessons_str.split(",")]
                    
                    # Thu thập lessons và lưu link
                    for lesson in lessons:
                        all_lessons.append(lesson)
                        lessons_with_links[lesson] = link
                
                    
                elif match_without_link:
                    topic, lessons_str = match_without_link.groups()
                    topic = topic.strip()
                    lessons_str = lessons_str.strip()
                    
                    # Tách các lesson bằng dấu phẩy
                    lessons = [lesson.strip() for lesson in lessons_str.split(",")]
                    
                    # Thu thập lessons (không có link)
                    for lesson in lessons:
                        all_lessons.append(lesson)
                    
                                    
                else:
                    continue
            
            # Đếm tần suất xuất hiện của từng lesson
            from collections import Counter
            lesson_counts = Counter(all_lessons)
            
            # Lấy 5 lesson xuất hiện nhiều nhất từ toàn bộ data
            top_lessons = [lesson for lesson, count in lesson_counts.most_common(5)]
            print(f"Top 5 lessons by frequency: {lesson_counts.most_common(5)}")
            
            # Tạo cấu trúc PDF chỉ với các topics chứa top 5 lessons
            topic_lessons_map = {}  # topic -> list of lessons
            topic_links_map = {}    # topic -> link
            
            for topic_data in topics:
                # Parse lại để lấy topic gốc
                match_with_link = re.match(r"Chủ đề\s*(.+?):\s*(.+?)\s*\((https?://.+?)\)", topic_data)
                match_without_link = re.match(r"Chủ đề\s*(.+?):\s*(.+?)$", topic_data)
                
                if match_with_link:
                    topic, lessons_str, link = match_with_link.groups()
                    topic = topic.strip()
                    lessons_str = lessons_str.strip()
                    lessons = [lesson.strip() for lesson in lessons_str.split(",")]
                    
                    # Chỉ lấy topics có chứa ít nhất 1 lesson trong top 5
                    filtered_lessons = [lesson for lesson in lessons if lesson in top_lessons]
                    
                    if filtered_lessons:
                        # Lấy tất cả lessons của topic này (không chỉ filtered)
                        if topic not in topic_lessons_map:
                            topic_lessons_map[topic] = []
                            topic_links_map[topic] = link
                        topic_lessons_map[topic] = lessons  # Lấy tất cả lessons của topic
                    
                elif match_without_link:
                    topic, lessons_str = match_without_link.groups()
                    topic = topic.strip()
                    lessons_str = lessons_str.strip()
                    lessons = [lesson.strip() for lesson in lessons_str.split(",")]
                    
                    # Chỉ lấy topics có chứa ít nhất 1 lesson trong top 5
                    filtered_lessons = [lesson for lesson in lessons if lesson in top_lessons]
                    
                    if filtered_lessons:
                        # Lấy tất cả lessons của topic này (không chỉ filtered)
                        if topic not in topic_lessons_map:
                            topic_lessons_map[topic] = []
                        topic_lessons_map[topic] = lessons  # Lấy tất cả lessons của topic
            
            # Tạo PDF structure từ topic_lessons_map (giới hạn tối đa 5 topics)
            topic_counter = 0
            for topic, lessons_list in topic_lessons_map.items():
                if topic_counter >= 5:  # Giới hạn tối đa 5 topics
                    break
                    
                # Loại bỏ duplicate lessons và giữ thứ tự gốc
                unique_lessons = []
                seen = set()
                for lesson in lessons_list:
                    if lesson not in seen:
                        unique_lessons.append(lesson)
                        seen.add(lesson)
                
                # Tạo content string với tất cả lessons của topic
                lessons_text = ", ".join(unique_lessons)
                if topic in topic_links_map:
                    content = f"{lessons_text} ({topic_links_map[topic]})"
                else:
                    content = lessons_text
                
                pdf_structure[topic] = {"content": [content]}
                topic_counter += 1
                

            # Hiển thị theo thứ tự (topics với top lessons)
            for topic in pdf_structure.keys():
                pdf.set_font("DejaVu", style="I")
                pdf.cell(0, 8, f"- {topic}", ln=True)
                pdf.set_font("DejaVu", size=11)
                
                lessons = pdf_structure[topic]["content"]
                lines_needed = len(lessons)
                check_and_add_page(pdf, lines_count=lines_needed)

                for lesson_content in lessons:
                    # Parse link từ lesson content (đã bao gồm link)
                    link_match = re.match(r"(.+?)\s*\((https?://.+?)\)", lesson_content)
                    if link_match:
                        lesson_text, link = link_match.groups()
                        pdf.cell(14)  # Indent 14 units
                        # Tính toán chiều rộng cần thiết cho text
                        text_width = pdf.get_string_width(f"• {lesson_text}")
                        pdf.cell(text_width, 8, f"• {lesson_text}", ln=False)
                        pdf.set_text_color(0, 0, 255)
                        pdf.cell(0, 8, " (Link bài luyện)", link=link, ln=True)
                        pdf.set_text_color(0, 0, 0)
                    else:
                        pdf.cell(14)  # Indent 14 units
                        pdf.cell(0, 8, f"• {lesson_content}", ln=True)

        pdf.multi_cell(0, 8, "Dựa trên các nội dung định hướng trên, thí sinh có thể truy cập đường liên kết được gợi ý đi kèm mỗi phần kiến thức và đăng nhập tài khoản thi chính thức do Ban Tổ chức cấp để luyện tập", ln=True)
    else:
        pdf.multi_cell(0, 8, "Thí sinh đã đáp ứng được các kỹ năng và kiến thức môn Toán cần thiết ở lứa tuổi này, BTC hy vọng thí sinh tiếp tục giữ vững và phát huy.", ln=True)

    pdf.ln(3)
    pdf.multi_cell(0, 8, f"Chúng tôi tin rằng với sự cố gắng và nỗ lực, thí sinh {row['Họ và tên']} sẽ tiếp tục đạt được nhiều thành tích cao hơn nữa.", ln=True)
    pdf.ln(1.6)
    pdf.multi_cell(0, 8, "Cảm ơn sự quan tâm và hy vọng chúng tôi sẽ tiếp tục nhận được sự ủng hộ và đồng hành của Quý thầy cô, Quý Phụ huynh và thí sinh trong những kỳ thi tiếp theo.", ln=True)
    pdf.ln(3)
    pdf.set_font("DejaVu", style="B")
    pdf.cell(0, 10, "BTC KỲ THI THÁCH THỨC TƯ DUY THUẬT TOÁN BEBRAS", ln=True, align="R")

    pdf.image(footer_image , x=0, y=281, w=210)

    pdf.output(pdf_path)

def process_excel(input_file, output_folder):
    """Hàm xử lý file Excel và tạo các file PDF"""
    # Đọc tất cả các sheet trong file Excel
    xls = pd.ExcelFile(input_file)

    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name)

        if "Lớp" not in df.columns or "Họ và tên" not in df.columns or "Mã trường" not in df.columns:
            print(f"Bỏ qua sheet {sheet_name} do không có cột 'Lớp', 'Họ và tên' hoặc 'Mã trường'.")
            continue

        # Xử lý từng học sinh
        for _, row in df.iterrows():
            school_code = str(row.get("Mã trường", "")).strip()
            school_name = str(row.get("Trường", "")).strip()
            province = str(row.get("Tỉnh/TP", "")).strip()
            ward = str(row.get("Xã/phường", "")).strip()
            address = str(row.get("Trường_Xã/phường_Tỉnh/TP", "")).strip()

            if school_code == "nan":
                folder_name = "DSHS không có mã trường"
            else:
                folder_name = f"{sanitize_filename(school_code)}-{sanitize_filename(address)}-Bebras2026-Thi thử 1"
            sheet_output_folder = os.path.join(output_folder, folder_name)
            os.makedirs(sheet_output_folder, exist_ok=True)

            # Tạo PDF cho từng học sinh
            pdf_generator(row, sheet_name, sheet_output_folder)


# Gọi hàm xử lý file Excel
if __name__ == "__main__":
    process_excel(input_excel, output_folder)
    print("Hoàn tất tạo file PDF!")