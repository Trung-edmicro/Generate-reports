import os
import pandas as pd
import threading
import queue
import time
import logging
from ai_comment_generator import ai_comment_generator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def processor_data(input_file1, input_file2):
    logging.info("Đọc dữ liệu từ file input...")
    df1 = pd.read_excel(input_file1, engine='openpyxl', dtype=str).fillna("")
    df1 = df1.rename(columns=lambda x: str(x).strip())
    df1["Câu trả lời"] = df1["Câu trả lời"].str.strip()
    df1["Đáp án"] = df1["Đáp án"].str.strip()
    df1['Tên trường'] = df1['Trường'].str.strip().str.replace(r'[^\w\s]', '', regex=True)
    
    df2 = pd.read_excel(input_file2, engine='openpyxl', dtype=str).fillna("")
    df2 = df2.rename(columns=lambda x: str(x).strip())
    logging.info("Hoàn thành đọc dữ liệu.")
    return df1, df2

def evaluate_answers(df1, df2, result_queue):
    logging.info("Bắt đầu đánh giá kết quả làm bài...")
    total_basic = (df2["Cấp độ nhận thức"].isin(["NB", "TH"])).sum()
    total_advanced = (df2["Cấp độ nhận thức"].isin(["VD", "VDC"])).sum()
    
    def evaluate_row(row):
        student_answers, correct_answers = row["Câu trả lời"], row["Đáp án"]
        if not student_answers.strip():
            return 0, 25, False, 0, 0, "", ""
        
        correct_count = sum(1 for i in range(25) if student_answers[i:i+1] == correct_answers[i:i+1])
        wrong_count = 25 - correct_count
        
        correct_basic = sum(1 for i in range(25) if student_answers[i:i+1] == correct_answers[i:i+1] and df2.iloc[i]["Cấp độ nhận thức"] in ["NB", "TH"])
        correct_advanced = sum(1 for i in range(25) if student_answers[i:i+1] == correct_answers[i:i+1] and df2.iloc[i]["Cấp độ nhận thức"] in ["VD", "VDC"])
        
        percent_basic = (correct_basic / total_basic * 100) if total_basic else 0
        percent_advanced = (correct_advanced / total_advanced * 100) if total_advanced else 0
        return correct_count, wrong_count, correct_count > 20, percent_basic, percent_advanced, "", ""
    
    df1[["correct_answer", "incorrect_answers", "pass", "basic_level", "advance_level", "review_text", "wrong_text"]] = pd.DataFrame(df1.apply(evaluate_row, axis=1).tolist(), index=df1.index)
    df1["basic_level"] = df1["basic_level"].round(0).astype(int).astype(str) + "%"
    df1["advance_level"] = df1["advance_level"].round(0).astype(int).astype(str) + "%"
    df1["pass"] = df1["pass"].replace({True: "X", False: ""})
    
    logging.info("Hoàn thành đánh giá kết quả.")
    result_queue.put(df1)

def generate_comments(df, result_queue):
    logging.info("Bắt đầu tạo comments...")
    num_rows = len(df)
    df['comments'] = ""
    
    for i in range(0, num_rows, 15):
        batch_df = df.iloc[i:min(i + 15, num_rows)].copy()
        batch_df = batch_df[batch_df["Câu trả lời"].str.strip() != ""]
        
        if not batch_df.empty:
            batch_df['comments'] = batch_df.apply(lambda row: ai_comment_generator(
                row["Họ và tên đệm"].strip() + " " + row["Tên"].strip(),
                row["correct_answer"], row["incorrect_answers"],
                row["basic_level"], row["advance_level"], row["wrong_text"]
            ), axis=1)
            df.loc[batch_df.index, 'comments'] = batch_df['comments']
            
            logging.info(f"Đã tạo comments cho {min(i + 15, num_rows)}/{num_rows} hàng")
            
            if i + 15 < num_rows:
                logging.info("Đợi 20 giây để tránh vượt quá giới hạn API...")
                time.sleep(20)
    
    logging.info("Hoàn thành tạo comments.")
    result_queue.put(df)

def save_results(df, output_excel, output_pdf_dir):
    logging.info(f"Lưu kết quả vào {output_excel}...")
    df.to_excel(output_excel, index=False, engine='openpyxl')
    os.makedirs(output_pdf_dir, exist_ok=True)
    logging.info("Hoàn thành lưu kết quả.")

def main():
    logging.info("Bắt đầu xử lý...")
    input_file1 = "data/input/input.xlsx"
    input_file2 = "data/input/matran.xlsx"
    output_excel = "data/output/output.xlsx"
    output_pdf_dir = "data/output/"
    
    df1, df2 = processor_data(input_file1, input_file2)
    result_queue = queue.Queue()
    
    eval_thread = threading.Thread(target=evaluate_answers, args=(df1, df2, result_queue))
    eval_thread.start()
    eval_thread.join()
    df1 = result_queue.get()
    
    comment_thread = threading.Thread(target=generate_comments, args=(df1, result_queue))
    comment_thread.start()
    comment_thread.join()
    df1 = result_queue.get()
    
    save_results(df1, output_excel, output_pdf_dir)
    logging.info(f"Đã xử lý và lưu kết quả vào file: {output_excel}")

if __name__ == '__main__':
    main()
