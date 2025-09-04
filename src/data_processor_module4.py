import os
import re
import time
import asyncio
import pandas as pd
import google.generativeai as genai
from dotenv import load_dotenv
from logger_config import logger
from utils.helpers import load_prompt

load_dotenv()
API_KEY_1 = os.getenv("API_KEY_1")
API_KEY_2 = os.getenv("API_KEY_2")

if not API_KEY_1 and API_KEY_2:
    raise ValueError("Vui lòng đặt biến môi trường API_KEY với API key của bạn.")

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

    basic_percent_list = []
    advanced_percent_list = []
    
    for i, col in enumerate(df1.columns):
        if i >= len(df1.iloc[0]):
            break
        if pd.notna(df1.iloc[0][col]):
            val = str(df1.iloc[0][col]).strip()
            val = val.replace("Ð", "Đ")  # Chuẩn hóa ký tự do xử lý đáp án câu Đúng/Sai bằng VBA trên Excel (tạm thời)

            if val in ['Đúng', 'Sai', 'Bỏ qua']:
                result_indices.append(i)
                
    # Process each student
    for index, row in df1.iterrows():
        wrong_questions = []
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
                    wrong_questions.append(str(i + 1))
                elif result == 'Bỏ qua':
                    skipped_count += 1
                    skipped.append(str(i + 1))
            else:
                print(f"Error: if pd.notna(row[col]) is False, {result}")

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

                if not matched_rows.empty:
                    subject = matched_rows["Môn"].values[0] if "Môn" in matched_rows.columns else ""
                    topic = matched_rows["Chủ đề"].values[0] if "Chủ đề" in matched_rows.columns else ""
                    lesson = matched_rows["Bài"].values[0] if "Bài" in matched_rows.columns else ""

                    if isinstance(subject, float):
                        subject = "" if pd.isna(subject) else str(subject)
                    if isinstance(topic, float):
                        topic = "" if pd.isna(topic) else str(topic)
                    if isinstance(lesson, float):
                        lesson = "" if pd.isna(lesson) else str(lesson)
                    
                    # Nhóm các bài theo từng chủ đề, môn học
                    if topic:
                        if "Môn" in df2.columns:
                            if subject not in grouped_dict:
                                grouped_dict[subject] = {}
                            if topic not in grouped_dict[subject]:
                                grouped_dict[subject][topic] = set()
                            grouped_dict[subject][topic].add(lesson)
                        else:  
                            if topic in grouped_dict:
                                grouped_dict[topic].add(lesson)
                            else:
                                grouped_dict[topic] = {lesson}

        # Format kết quả
        if "Môn" in df2.columns:
            formatted_content = "; ".join([
                f"{subject} - {topic}: {' - '.join(sorted(lessons))}"
                for subject, topics in grouped_dict.items()
                for topic, lessons in topics.items()
            ])
        else:
            formatted_content = "; ".join([
                f"{topic}: {' - '.join(sorted(lessons))}"
                for topic, lessons in grouped_dict.items()
            ])

        # formatted_content = "; ".join([f"{topic}: {' - '.join(sorted(lessons))}" for topic, lessons in topic_dict.items()])

        improvement_content.append(formatted_content if formatted_content else "")
    
    if len(df1.columns) >= 11:
        column_indices = list(range(11))  # Columns A through K (0-10)
        new_df = df1.iloc[:, column_indices].copy()
    else:
        new_df = df1.copy()

    # Add the new column to the dataframe
    new_df["Đúng"] = correct_list
    new_df["Sai"] = wrong_list
    new_df["Bỏ qua"] = skip_list

    new_df["Mức độ kiến thức cơ bản đạt được"] = basic_percent_list
    new_df["Mức độ kiến thức nâng cao đạt được"] = advanced_percent_list

    # Handle class ranking
    if "Lớp" in new_df.columns:
        # Rank within class
        new_df["Thứ hạng trong lớp_rank"] = new_df.groupby("Lớp")["Điểm"].rank(ascending=False, method="min").astype("Int64")
        new_df["Thứ hạng trong lớp"] = new_df["Thứ hạng trong lớp_rank"].astype(str) + "/" + new_df.groupby("Lớp")["Điểm"].transform("count").astype(str)

        # Extract grade level and handle grade level ranking
        new_df["Thứ hạng trong khối_rank"] = new_df.groupby(new_df["Lớp"].str.extract(r'(\d+)')[0])["Điểm"].rank(ascending=False, method="min").astype("Int64")
        new_df["Thứ hạng trong khối"] = new_df["Thứ hạng trong khối_rank"].astype(str) + "/" + new_df.groupby(new_df["Lớp"].str.extract(r'(\d+)')[0])["Điểm"].transform("count").astype(str)

        # Drop temporary columns
        new_df = new_df.drop(columns=["Thứ hạng trong lớp_rank", "Thứ hạng trong khối_rank"])

    new_df["Nội dung cần cải thiện"] = improvement_content

    return new_df

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
        gemini_api_keys = [API_KEY_1, API_KEY_2]

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

async def process_feedbacks(new_df):
    semaphore = asyncio.Semaphore(15)  # Giới hạn 15 request cùng lúc
    tasks = []
    
    logger.info("Bắt đầu tạo nhận xét cho học sinh...")

    for index, row in new_df.iterrows():
        student_name = row["Họ và tên"]
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