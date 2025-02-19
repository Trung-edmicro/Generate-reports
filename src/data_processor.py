import os
import pandas as pd
import numpy as np
import multiprocessing as mp

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

    return df1, df2

def evaluate_chunk(rows, df2, total_basic, total_advanced):
    """
    Xử lý một nhóm dữ liệu (~15 học sinh) trong tiến trình song song.
    """
    results = []
    for row in rows:
        student_answers = row["Câu trả lời"]
        correct_answers = row["Đáp án"]

        if not student_answers.strip():
            results.append([0, 25, False, 0, 0, "", ""])
            continue

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
            else:
                correct_count += 1
                if df2.iloc[i]["Cấp độ nhận thức"] in ["NB", "TH"]:
                    correct_basic += 1
                elif df2.iloc[i]["Cấp độ nhận thức"] in ["VD", "VDC"]:
                    correct_advanced += 1

        percent_basic = (correct_basic / total_basic * 100) if total_basic > 0 else 0
        percent_advanced = (correct_advanced / total_advanced * 100) if total_advanced > 0 else 0

        results.append([correct_count, wrong_count, correct_count > 20, percent_basic, percent_advanced, "", ""])

    return results

def evaluate_answers(df1, df2):
    """
    Sử dụng multiprocessing để xử lý đánh giá câu trả lời theo nhóm.
    """
    total_basic = (df2["Cấp độ nhận thức"].isin(["NB", "TH"])).sum()
    total_advanced = (df2["Cấp độ nhận thức"].isin(["VD", "VDC"])).sum()

    # Chia dữ liệu thành danh sách nhóm (15 dòng mỗi nhóm)
    chunks = [df1.iloc[i:i+15].to_dict(orient="records") for i in range(0, len(df1), 15)]

    # Chạy multiprocessing
    with mp.Pool(mp.cpu_count()) as pool:
        results = pool.starmap(evaluate_chunk, [(chunk, df2, total_basic, total_advanced) for chunk in chunks])
        pool.close()
        pool.join()

    # Gộp kết quả lại
    flattened_results = [item for sublist in results for item in sublist]

    # Thêm dữ liệu vào dataframe
    df1[[
        "Số câu trả lời đúng",
        "Số câu trả lời sai",
        "Học sinh trên 20 điểm",
        "Mức độ kiến thức cơ bản đạt được",
        "Mức độ kiến thức nâng cao đạt được",
        "Nhận xét về kết quả bài thi",
        "Nội dung câu trả lời sai"
    ]] = pd.DataFrame(flattened_results, index=df1.index)

    # Chuyển phần trăm thành dạng hiển thị %
    df1["Mức độ kiến thức cơ bản đạt được"] = df1["Mức độ kiến thức cơ bản đạt được"].round(0).astype(int).astype(str) + "%"
    df1["Mức độ kiến thức nâng cao đạt được"] = df1["Mức độ kiến thức nâng cao đạt được"].round(0).astype(int).astype(str) + "%"

    df1["Học sinh trên 20 điểm"] = df1["Học sinh trên 20 điểm"].replace({True: "X", False: ""})

    return df1

def save_results(df1, output_excel, output_pdf_dir):
    df1.to_excel(output_excel, index=False, engine='openpyxl')
    os.makedirs(output_pdf_dir, exist_ok=True)
