import os
import PyPDF2

def merge_pdfs_in_folder(folder_path, output_filename="tong_hop.pdf"):
    pdf_writer = PyPDF2.PdfMerger()
    
    pdf_files = sorted(
        [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")],
        key=lambda x: os.path.getctime(os.path.join(folder_path, x))
    )
    
    for pdf in pdf_files:
        pdf_path = os.path.join(folder_path, pdf)
        pdf_writer.append(pdf_path)
    
    if pdf_files:
        output_path = os.path.join(folder_path, output_filename)
        pdf_writer.write(output_path)
        pdf_writer.close()
        print(f"Merged {len(pdf_files)} PDFs into {output_path}")
    else:
        print(f"No PDFs found in {folder_path}")

def process_all_folders(base_folder):
    for i in range(1, 91):  # Duyệt từ folder 1 đến 90
        folder_path = os.path.join(base_folder, str(i))
        if os.path.isdir(folder_path):
            merge_pdfs_in_folder(folder_path)

# Thay 'your_base_folder' bằng đường dẫn thực tế
base_folder = "output"
process_all_folders(base_folder)
