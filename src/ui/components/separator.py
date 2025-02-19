import sys
from PyQt5.QtWidgets import QWidget, QHBoxLayout, QListWidget, QStackedWidget, QPushButton, QVBoxLayout, QLabel, QFrame

def create_separator():
    separator = QFrame()
    separator.setFrameShape(QFrame.HLine)
    separator.setFrameShadow(QFrame.Sunken)
    return separator