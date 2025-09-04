import os
import pandas as pd

def processor_data(input_file1, input_file2):
    # Đọc file kết quả làm bài
    df1 = pd.read_excel(input_file1, engine='openpyxl', header=0, dtype=str)
    df1 = df1.rename(columns=lambda x: str(x).strip())
    df1["Câu trả lời"] = df1["Câu trả lời"].fillna("").astype(str).str.strip()
    df1["Đáp án"] = df1["Đáp án"].fillna("").astype(str).str.strip()

    df1['Tên trường'] = df1['Trường'].str.strip()  
    df1['Tên trường'] = df1['Tên trường'].str.replace(r'[^\w\s]', '', regex=True)

    # Đọc file ma trận kiến thức
    df2 = pd.read_excel(input_file2, engine='openpyxl', header=0)
    df2 = df2.rename(columns=lambda x: str(x).strip())
    df2["Cấp độ nhận thức"] = df2["Cấp độ nhận thức"].fillna("").astype(str).str.strip()
    df2["Chủ đề"] = df2["Chủ đề"].fillna("").astype(str).str.strip()
    df2["Nội dung"] = df2["Nội dung"].fillna("").astype(str).str.strip()
    df2["Bài"] = df2["Bài"].fillna("").astype(str).str.strip()

    return df1, df2

def evaluate_answers(df1, df2):
    """
    Đánh giá kết quả làm bài dựa trên đáp án và ma trận kiến thức.
    """
    total_basic = (df2["Cấp độ nhận thức"].isin(["NB", "TH"])).sum()
    total_advanced = (df2["Cấp độ nhận thức"].isin(["VD", "VDC"])).sum()

    def evaluate_row(row):
        student_answers = row["Câu trả lời"]
        correct_answers = row["Đáp án"]

        if not student_answers.strip():
            return pd.Series([0, 25, False, 0, 0, "", ""])

        correct_count = 0
        wrong_count = 0
        correct_basic = 0
        correct_advanced = 0
        correct_details = set()
        wrong_details = set()

        for i in range(25):
            student_choice = student_answers[i] if i < len(student_answers) else ""
            correct_choice = correct_answers[i] if i < len(correct_answers) else ""

            if student_choice in [".", "*", ""] or student_choice != correct_choice:
                wrong_count += 1
                if i < len(df2):
                    topic = df2.iloc[i]["Chủ đề"].strip() if pd.notna(df2.iloc[i]["Chủ đề"]) else ""
                    content = df2.iloc[i]["Nội dung"].strip() if pd.notna(df2.iloc[i]["Nội dung"]) else ""
                    exercise = df2.iloc[i]["Bài"].strip() if pd.notna(df2.iloc[i]["Bài"]) else ""
                    link = df2.iloc[i]["Link bài luyện"].strip() if pd.notna(df2.iloc[i]["Link bài luyện"]) else ""

                    if exercise and content:
                        wrong_details.add((topic, content, exercise, link))

            else:
                correct_count += 1
                if df2.iloc[i]["Cấp độ nhận thức"] in ["NB", "TH"]:
                    correct_basic += 1
                elif df2.iloc[i]["Cấp độ nhận thức"] in ["VD", "VDC"]:
                    correct_advanced += 1

                if i < len(df2):
                    topic = df2.iloc[i]["Chủ đề"].strip() if pd.notna(df2.iloc[i]["Chủ đề"]) else ""
                    content = df2.iloc[i]["Nội dung"].strip() if pd.notna(df2.iloc[i]["Nội dung"]) else ""
                    exercise = df2.iloc[i]["Bài"].strip() if pd.notna(df2.iloc[i]["Bài"]) else ""

                    if exercise and content:
                        correct_details.add((topic, content, exercise))

        percent_basic = (correct_basic / total_basic * 100) if total_basic > 0 else 0
        percent_advanced = (correct_advanced / total_advanced * 100) if total_advanced > 0 else 0

        # Danh sách nội dung các câu đúng
        review_text = "; ".join([
            f"{topic} - {content}: {exercise}"
            for topic, content, exercise in sorted(correct_details)
        ]) if correct_count > 12 else ""

        # Danh sách nội dung các câu sai
        wrong_text = "; ".join([
            f"{topic} - {content}: {exercise} ({link})" if link else f"{exercise}"
            for topic, content, exercise, link in sorted(wrong_details)
        ]) if wrong_details else ""

        return pd.Series([correct_count, wrong_count, correct_count > 20, percent_basic, percent_advanced, review_text, wrong_text])

    # Áp dụng đánh giá cho từng học sinh
    df1[[
        "Số câu trả lời đúng", 
        "Số câu trả lời sai", 
        "Học sinh trên 20 điểm", 
        "Mức độ kiến thức cơ bản đạt được", 
        "Mức độ kiến thức nâng cao đạt được", 
        "Nhận xét về kết quả bài thi", 
        "Nội dung câu trả lời sai"
    ]] = df1.apply(evaluate_row, axis=1)

    df1["Mức độ kiến thức cơ bản đạt được"] = df1["Mức độ kiến thức cơ bản đạt được"].round(0).astype(int).astype(str) + "%"
    df1["Mức độ kiến thức nâng cao đạt được"] = df1["Mức độ kiến thức nâng cao đạt được"].round(0).astype(int).astype(str) + "%"

    df1["Học sinh trên 20 điểm"] = df1["Học sinh trên 20 điểm"].replace({True: "X", False: ""})

    # **XẾP HẠNG TRONG LỚP**
    df1["Số câu trả lời đúng"] = df1["Số câu trả lời đúng"].astype(int)
    df1["Thứ hạng trong lớp"] = df1.groupby("Lớp")["Số câu trả lời đúng"].rank(ascending=False, method="dense").astype("Int64")
    
    df1["Thứ hạng trong lớp"] = df1["Thứ hạng trong lớp"].astype(str) + "/" + df1.groupby("Lớp")["Số câu trả lời đúng"].transform("count").astype(str)

    # **XẾP HẠNG TRONG KHỐI**
    df1["Thứ hạng trong khối"] = df1.groupby(df1["Lớp"].str.extract(r'(\d+)')[0])["Số câu trả lời đúng"].rank(ascending=False, method="dense").astype("Int64")
    
    df1["Thứ hạng trong khối"] = df1["Thứ hạng trong khối"].astype(str) + "/" + df1.groupby(df1["Lớp"].str.extract(r'(\d+)')[0])["Số câu trả lời đúng"].transform("count").astype(str)

    df1.loc[df1["Câu trả lời"].str.strip() == "", ["Thứ hạng trong lớp", "Thứ hạng trong khối"]] = "Không thi"

    return df1

def save_results(df1, output_excel, output_pdf_dir):
    df1.to_excel(output_excel, index=False, engine='openpyxl')
    os.makedirs(output_pdf_dir, exist_ok=True)
