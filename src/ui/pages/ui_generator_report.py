import time
from PyQt5.QtWidgets import (
    QWidget, QLabel, QPushButton, QVBoxLayout, QHBoxLayout, QLineEdit, QFileDialog, QProgressBar, QRadioButton
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QFont
from ..components.separator import create_separator

class ProcessingThread(QThread):
    progress = pyqtSignal(int)
    finished_signal = pyqtSignal(bool)
    
    def __init__(self):
        super().__init__()
        self.is_canceled = False
        self.is_paused = False

    def run(self):
        for i in range(1, 101):
            if self.is_canceled:
                self.finished_signal.emit(False)
                return

            while self.is_paused:
                time.sleep(0.1)

            time.sleep(0.05)
            self.progress.emit(i)

        self.finished_signal.emit(True)

    def pause(self):
        self.is_paused = True

    def resume(self):
        self.is_paused = False

    def cancel(self):
        self.is_canceled = True

class GeneratorReport(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
    
    def initUI(self):
        main_layout = QVBoxLayout()
        separator = create_separator()

        title_process = QLabel("Xử lý kết quả")
        title_process.setFont(QFont("Arial", 12))

        # File Kết Quả
        label_result = QLabel("File Kết Quả:")
        file_result_layout = QHBoxLayout()
        self.file_path_result = QLineEdit()
        self.file_path_result.setReadOnly(True)
        btn_choose_result = QPushButton("Chọn file")
        btn_choose_result.clicked.connect(lambda: self.choose_file(self.file_path_result))

        file_result_layout.addWidget(self.file_path_result)
        file_result_layout.addWidget(btn_choose_result)

        # File Ma Trận
        label_matrix = QLabel("File Ma Trận:")
        file_matrix_layout = QHBoxLayout()
        self.file_path_matrix = QLineEdit()
        self.file_path_matrix.setReadOnly(True)
        btn_choose_matrix = QPushButton("Chọn file")
        btn_choose_matrix.clicked.connect(lambda: self.choose_file(self.file_path_matrix))

        file_matrix_layout.addWidget(self.file_path_matrix)
        file_matrix_layout.addWidget(btn_choose_matrix)
        
        self.process_button = QPushButton("Xử lý")
        self.process_button.setEnabled(False)
        self.process_button.clicked.connect(self.start_processing)

        self.cancel_button = QPushButton("Tạm dừng")
        self.cancel_button.setEnabled(False)
        self.cancel_button.clicked.connect(self.toggle_pause_or_restart)

        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setAlignment(Qt.AlignCenter)

        process_layout = QHBoxLayout()
        process_layout.addWidget(self.process_button)
        process_layout.addWidget(self.cancel_button)
        process_layout.addWidget(self.progress_bar)

        # Xử lý file mẫu
        label_report_template = QLabel("File Báo Cáo Mẫu:")
        radio_default = QRadioButton("Mặc định")
        radio_custom = QRadioButton("Khác (Chỉ file .docx)")

        radio_default.setChecked(True)
        
        report_layout = QHBoxLayout()
        report_layout.addWidget(label_report_template)
        report_layout.addWidget(radio_default)
        report_layout.addWidget(radio_custom)
        report_layout.addStretch(1)

        file_report_template_layout = QHBoxLayout()
        self.file_path_report_template = QLineEdit()
        self.file_path_report_template.setReadOnly(True)
        btn_choose_template = QPushButton("Chọn file")
        btn_choose_template.clicked.connect(lambda: self.choose_file(self.file_path_report_template))

        file_report_template_layout.addWidget(self.file_path_report_template)
        file_report_template_layout.addWidget(btn_choose_template)       
        self.file_path_report_template.setVisible(False)
        btn_choose_template.setVisible(False)

        # Hàm xử lý khi chọn radio button
        def toggle_radio():
            is_default = radio_default.isChecked()
            is_custom = radio_custom.isChecked()

            if is_default:
                self.file_path_report_template.setVisible(False)
                btn_choose_template.setVisible(False)

            if is_custom:
                self.file_path_report_template.setVisible(True)
                btn_choose_template.setVisible(True)
                
        radio_default.toggled.connect(toggle_radio)
        radio_custom.toggled.connect(toggle_radio)

        main_layout.addWidget(title_process)
        main_layout.addWidget(label_result)
        main_layout.addLayout(file_result_layout)
        main_layout.addWidget(label_matrix)
        main_layout.addLayout(file_matrix_layout)
        main_layout.addLayout(process_layout)
        main_layout.addWidget(separator)
        main_layout.addLayout(report_layout)
        main_layout.addLayout(file_report_template_layout)
      
        main_layout.addStretch()

        self.setLayout(main_layout)

    def choose_file(self, file_path_widget):
        file_name, _ = QFileDialog.getOpenFileName(self, "Chọn file", "", "All Files (*);;Text Files (*.txt)")
        if file_name:
            self.process_button.setEnabled(True)
            file_path_widget.setText(file_name)

    def start_processing(self):
        if hasattr(self, 'worker_thread') and self.worker_thread.isRunning():
            self.worker_thread.resume()
            self.process_button.setText("Đang xử lý...")
            self.cancel_button.setText("Tạm dừng")
            return

        self.process_button.setEnabled(False)
        self.cancel_button.setEnabled(True)
        self.cancel_button.setText("Tạm dừng")

        self.progress_bar.setValue(0)

        self.worker_thread = ProcessingThread()
        self.worker_thread.progress.connect(self.update_progress)
        self.worker_thread.finished_signal.connect(self.processing_done)
        self.worker_thread.start()

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def processing_done(self, completed):
        self.process_button.setEnabled(True)
        self.process_button.setText("Hoàn thành")
        self.cancel_button.setText("Xử lý lại")

    def toggle_pause_or_restart(self):
        if self.cancel_button.text() == "Tạm dừng":
            self.worker_thread.pause()
            self.process_button.setText("Xử lý lại")
            self.cancel_button.setText("Tiếp tục")
        elif self.cancel_button.text() == "Tiếp tục":
            self.worker_thread.resume()
            self.cancel_button.setText("Tạm dừng")
            self.process_button.setText("Đang xử lý...")
        elif self.cancel_button.text() == "Xử lý lại":
            self.start_processing()
            self.process_button.setText("Xử lý")
            self.cancel_button.setText("Tạm dừng")
