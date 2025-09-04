import os
import pandas as pd
import asyncio
from logger_config import logger
from data_processor_module4 import handle_sheet, process_feedbacks

def processor(input_file1, input_file2, sheet_names=None):
    logger.info("Bắt đầu xử lý file Excel...")

    try:
        if sheet_names is None:
            xls = pd.ExcelFile(input_file1)
            sheet_names = xls.sheet_names
            logger.info(f"Tìm thấy {len(sheet_names)} sheet: {sheet_names}")

        result_dfs = {}

        for sheet_name in sheet_names:
            logger.info(f"Đọc dữ liệu từ sheet: {sheet_name}")

            df1 = pd.read_excel(input_file1, sheet_name=sheet_name)
            df2 = pd.read_excel(input_file2, sheet_name=sheet_name)

            logger.info("Bắt đầu xử lý sheet...")
            result = handle_sheet(df1, df2)

            logger.info("Bắt đầu tạo nhận xét cho học sinh...")
            result_feedbacks = asyncio.run(process_feedbacks(result))

            result_dfs[sheet_name] = result_feedbacks

        os.makedirs("data/output", exist_ok=True)
        output_file = "data/output/output.xlsx"

        with pd.ExcelWriter(output_file) as writer:
            for sheet_name, df in result_dfs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        logger.info(f"Kết quả đã lưu vào {output_file}")
        return result_dfs

    except Exception as e:
        logger.error(f"Lỗi khi xử lý file: {e}")
        print(f"Error processing sheets: {e}")

# Run the function to process all sheets
if __name__ == "__main__":
    input_file1 = "data/input/inputToHop.xlsx"
    input_file2 = "data/input/matranToHop.xlsx"
    output_pdf_dir = "data/output/Tổng hợp kết quả/"

    try:
        # Xử lý tất cả các sheet
        results = processor(input_file1, input_file2)

    except Exception as e:
        print(f"Error processing sheets: {e}")