import os
import pandas as pd
import asyncio
import multiprocessing as mp
import json
import sys
import traceback
import google.generativeai as genai
from google.oauth2 import service_account
from dotenv import load_dotenv
from logger_config import logger
from data_processor_module4 import handle_sheet, process_feedbacks, process_feedbacks_multiprocessing
from service_account_processor import process_feedbacks_service_account, sa_processor

load_dotenv()

def test_service_account():
    """
    Test Service Account với một prompt đơn giản
    """
    logger.info("🧪 Bắt đầu test Service Account...")
    
    try:
        # Load Service Account configuration
        SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
        SERVICE_ACCOUNT_KEY_JSON = os.getenv("SERVICE_ACCOUNT_KEY_JSON")
        
        service_account_credentials = None
        
        if SERVICE_ACCOUNT_KEY_JSON:
            try:
                service_account_info = json.loads(SERVICE_ACCOUNT_KEY_JSON)
                service_account_credentials = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                logger.info("✅ Đã tải Service Account từ JSON string")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tải Service Account từ JSON: {e}")
                return False
                
        elif SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
            try:
                service_account_credentials = service_account.Credentials.from_service_account_file(
                    SERVICE_ACCOUNT_FILE,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                logger.info(f"✅ Đã tải Service Account từ file: {SERVICE_ACCOUNT_FILE}")
            except Exception as e:
                logger.error(f"❌ Lỗi khi tải Service Account từ file: {e}")
                return False
        else:
            # Thử tạo service account từ env vars riêng lẻ
            project_id = os.getenv('PROJECT_ID')
            private_key = os.getenv('PRIVATE_KEY')
            client_email = os.getenv('CLIENT_EMAIL')
            
            if project_id and private_key and client_email:
                try:
                    # Tạo service account info từ env vars
                    service_account_info = {
                        "type": os.getenv('TYPE', 'service_account'),
                        "project_id": project_id,
                        "private_key_id": os.getenv('PRIVATE_KEY_ID'),
                        "private_key": private_key.replace('\\n', '\n'),  # Fix newlines
                        "client_email": client_email,
                        "client_id": os.getenv('CLIENT_ID', ''),
                        "auth_uri": os.getenv('AUTH_URI', 'https://accounts.google.com/o/oauth2/auth'),
                        "token_uri": os.getenv('TOKEN_URI', 'https://oauth2.googleapis.com/token'),
                        "auth_provider_x509_cert_url": os.getenv('AUTH_PROVIDER_X509_CERT_URL'),
                        "client_x509_cert_url": os.getenv('CLIENT_X509_CERT_URL'),
                        "universe_domain": os.getenv('UNIVERSE_DOMAIN', 'googleapis.com')
                    }
                    

                    
                    service_account_credentials = service_account.Credentials.from_service_account_info(
                        service_account_info,
                        scopes=[
                            'https://www.googleapis.com/auth/cloud-platform',
                            'https://www.googleapis.com/auth/generative-language'
                        ]
                    )
                    logger.info("✅ Đã tạo Service Account từ environment variables")
                except Exception as e:
                    logger.error(f"❌ Lỗi khi tạo Service Account từ env vars: {e}")
                    return False
            else:
                logger.error("❌ Không tìm thấy thông tin Service Account trong .env")
                return False
            
        # Configure Gemini với Service Account
        genai.configure(credentials=service_account_credentials)
        model = genai.GenerativeModel('gemini-2.0-flash')
        
        # Test với prompt đơn giản
        test_prompt = "Xin chào! Hãy trả lời ngắn gọn: Bạn có thể giúp tôi tạo nhận xét cho học sinh không?"
        
        logger.info("🚀 Đang gửi test prompt tới Gemini...")
        
        response = model.generate_content(test_prompt)
        response.resolve()
        
        test_result = response.text
        
        logger.info("✅ Service Account hoạt động bình thường!")
        logger.info(f"📝 Response từ Gemini: {test_result[:200]}...")
        
        print("="*60)
        print("🎉 SERVICE ACCOUNT TEST - THÀNH CÔNG!")
        print("="*60)
        print(f"📝 Response: {test_result}")
        print("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Service Account test thất bại: {e}")
        
        print("="*60)
        print("💥 SERVICE ACCOUNT TEST - THẤT BẠI!")
        print("="*60)
        print(f"❌ Lỗi: {e}")
        print("="*60)
        
        # Gợi ý troubleshooting
        print("🔧 TROUBLESHOOTING:")
        print("1. Kiểm tra file .env có SERVICE_ACCOUNT_FILE hoặc SERVICE_ACCOUNT_KEY_JSON")
        print("2. Đảm bảo Service Account có quyền truy cập Gemini API")
        print("3. Kiểm tra project đã enable Generative AI API")
        print("4. Verify service account key chưa bị expired")
        print("="*60)
        
        return False

def processor_async(input_file1, input_file2, sheet_names=None):
    """
    Xử lý với phương pháp async (phương pháp cũ)
    """
    logger.info("Bắt đầu xử lý file Excel với phương pháp async...")

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

            # logger.info("Bắt đầu tạo nhận xét cho học sinh bằng async...")
            # result_feedbacks = asyncio.run(process_feedbacks(result))

            # result_dfs[sheet_name] = result_feedbacks

            result_dfs[sheet_name] = result


        return result_dfs

    except Exception as e:
        logger.error(f"Lỗi khi xử lý file với async: {e}")
        raise

def processor_multiprocessing(input_file1, input_file2, sheet_names=None):
    """
    Xử lý với multiprocessing
    """
    logger.info("Bắt đầu xử lý file Excel với multiprocessing...")

    try:
        if sheet_names is None:
            xls = pd.ExcelFile(input_file1)
            sheet_names = xls.sheet_names
            logger.info(f"Tìm thấy {len(sheet_names)} sheet: {sheet_names}")
            xls.close()  # Đóng file để tránh memory leak

        result_dfs = {}

        for sheet_name in sheet_names:
            logger.info(f"🔄 Xử lý sheet: {sheet_name}")

            try:
                df1 = pd.read_excel(input_file1, sheet_name=sheet_name)
                df2 = pd.read_excel(input_file2, sheet_name=sheet_name)

                logger.info(f"📊 Sheet {sheet_name}: {len(df1)} học sinh")
                
                # Xử lý sheet (không có AI feedback)
                result = handle_sheet(df1, df2)

                # Tạo nhận xét chỉ bằng Service Account
                logger.info("🏢 Bắt đầu tạo nhận xét với Service Account...")
                result_feedbacks = process_feedbacks_service_account(result)

                result_dfs[sheet_name] = result_feedbacks
                logger.info(f"✅ Hoàn thành sheet {sheet_name}")

            except Exception as sheet_error:
                logger.error(f"❌ Lỗi xử lý sheet {sheet_name}: {sheet_error}")
                # Tiếp tục với sheet khác thay vì dừng hoàn toàn
                continue

        if not result_dfs:
            raise ValueError("Không có sheet nào được xử lý thành công!")

        return result_dfs

    except Exception as e:
        logger.error(f"Lỗi khi xử lý file với multiprocessing: {e}")
        raise

def processor(input_file1, input_file2, sheet_names=None, use_multiprocessing=True):
    """
    Hàm chính để xử lý file Excel và tạo nhận xét cho học sinh
    
    Args:
        input_file1 (str): Đường dẫn file kết quả làm bài
        input_file2 (str): Đường dẫn file ma trận kiến thức  
        sheet_names (list, optional): Danh sách tên sheet cần xử lý. Nếu None thì xử lý tất cả
        use_multiprocessing (bool): True để dùng multiprocessing (KHUYẾN NGHỊ), False để dùng async
    
    Returns:
        dict: Dictionary chứa kết quả theo từng sheet
    """
    logger.info(f"Bắt đầu xử lý file Excel với {'multiprocessing' if use_multiprocessing else 'async'}...")

    try:
        # Chọn phương pháp xử lý
        if use_multiprocessing:
            result_dfs = processor_multiprocessing(input_file1, input_file2, sheet_names)
        else:
            result_dfs = processor_async(input_file1, input_file2, sheet_names)

        # Lưu kết quả
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
        raise

# Run the function to process all sheets
def main():
    """Hàm main chính của hệ thống"""
    import sys
    
    # Kiểm tra argument để test Service Account
    if len(sys.argv) > 1 and sys.argv[1] == "test_sa":
        print("🧪 TESTING SERVICE ACCOUNT...")
        success = test_service_account()
        if success:
            print("✅ Service Account test PASSED!")
        else:
            print("❌ Service Account test FAILED!")
        sys.exit(0)
    
    print("🚀 Bắt đầu hệ thống tạo báo cáo với Service Account...")
    
    # Kiểm tra Service Account
    if not sa_processor.service_account_creds:
        print("❌ Service Account không khả dụng! Vui lòng kiểm tra file .env")
        return
    else:
        stats = sa_processor.get_stats()
        print(f"✅ Service Account sẵn sàng: {stats}")
    
    # Kiểm tra các file input có sẵn
    available_files = {
        "DataBebras.xlsx": "data/input/BebrasV1/DataBebras0611.xlsx",
        "MatranBebras.xlsx": "data/input/BebrasV1/MatranBebras0611.xlsx"
        # "input_bebras1.1.xlsx": "data/input/input_bebras1.1.xlsx",
        # "MatranBebras1.1.xlsx": "data/input/MatranBebras1.1.xlsx"
    }
    
    print("📁 Kiểm tra files có sẵn:")
    found_files = {}
    for name, path in available_files.items():
        if os.path.exists(path):
            print(f"✅ {name}")
            found_files[name] = path
        else:
            print(f"❌ {name}")
    
    if not found_files:
        print("\n❌ Không tìm thấy file input nào!")
        print("Vui lòng đảm bảo có ít nhất một cặp file (data + matran) trong data/input/")
        return
    
    # Chọn file để xử lý (ưu tiên DataBebras.xlsx)
    if "DataBebras.xlsx" in found_files and "MatranBebras.xlsx" in found_files:
        input_file1 = found_files["DataBebras.xlsx"]
        input_file2 = found_files["MatranBebras.xlsx"]
        print(f"\n📊 Sử dụng: DataBebras.xlsx + MatranBebras.xlsx")
    elif "input_bebras1.1.xlsx" in found_files and "MatranBebras1.1.xlsx" in found_files:
        input_file1 = found_files["input_bebras1.1.xlsx"]
        input_file2 = found_files["MatranBebras1.1.xlsx"]
        print(f"\n📊 Sử dụng: input_bebras1.1.xlsx + MatranBebras1.1.xlsx")
    else:
        print("\n❌ Không tìm thấy cặp file data + matran hợp lệ!")
        return

    try:
        print("⚙️ Đang xử lý với multiprocessing...")
        
        # Xử lý với multiprocessing (mặc định)
        results = processor(input_file1, input_file2, use_multiprocessing=True)
        
        print(f"✅ Hoàn thành! Đã xử lý {len(results)} sheet")
        print("📄 Kết quả đã lưu tại: data/output/output.xlsx")
        
    except FileNotFoundError as e:
        print(f"❌ Không tìm thấy file: {e}")
        print("Vui lòng đảm bảo các file input tồn tại trong thư mục data/input/")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Thiết lập multiprocessing cho Windows (phải có trước khi import multiprocessing functions)
    try:
        mp.set_start_method('spawn', force=True)
    except RuntimeError:
        pass  # Đã được set rồi
    
    main()