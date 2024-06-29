import sys
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QPushButton, QFileDialog,
                             QProgressBar, QTextEdit, QVBoxLayout, QWidget, QLabel,
                             QComboBox, QHBoxLayout, QSpinBox, QDoubleSpinBox, QCheckBox,
                             QGroupBox, QFormLayout, QMessageBox)
from PyQt5.QtCore import QThread, pyqtSignal, Qt
from .processor import DataProcessor


class DataProcessorThread(QThread):
    progress = pyqtSignal(int)
    log = pyqtSignal(str)

    def __init__(self, input_dir, output_file, use_sample, sample_frac, n_workers, file_format, chunk_size):
        super().__init__()
        self.input_dir = input_dir
        self.output_file = output_file
        self.use_sample = use_sample
        self.sample_frac = sample_frac
        self.n_workers = n_workers
        self.file_format = file_format
        self.chunk_size = chunk_size

    def run(self):
        processor = DataProcessor(
            self.input_dir, self.output_file, self.use_sample,
            self.sample_frac, self.n_workers, self.file_format, self.chunk_size
        )
        processor.process_data(
            progress_callback=self.progress.emit,
            log_callback=self.log.emit
        )


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        self.setWindowTitle('BACI Data Analyzer Tool')
        self.setGeometry(100, 100, 800, 600)

        central_widget = QWidget()
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

        # Input and Output Selection
        io_group = QGroupBox("Input/Output")
        io_layout = QFormLayout()
        io_group.setLayout(io_layout)

        self.input_button = QPushButton('Select Input Directory')
        self.input_button.clicked.connect(self.select_input)
        io_layout.addRow("Input Directory:", self.input_button)

        self.output_button = QPushButton('Select Output File')
        self.output_button.clicked.connect(self.select_output)
        io_layout.addRow("Output File:", self.output_button)

        main_layout.addWidget(io_group)

        # Processing Options
        options_group = QGroupBox("Processing Options")
        options_layout = QFormLayout()
        options_group.setLayout(options_layout)

        self.use_sample_checkbox = QCheckBox('Use Sample')
        self.use_sample_checkbox.setChecked(True)
        options_layout.addRow("Use Sample:", self.use_sample_checkbox)

        self.sample_frac = QDoubleSpinBox()
        self.sample_frac.setRange(0.01, 1.0)
        self.sample_frac.setSingleStep(0.01)
        self.sample_frac.setValue(0.01)
        options_layout.addRow("Sample Fraction:", self.sample_frac)

        self.n_workers = QSpinBox()
        self.n_workers.setRange(1, os.cpu_count())
        self.n_workers.setValue(os.cpu_count() // 2)
        options_layout.addRow("Number of Workers:", self.n_workers)

        self.chunk_size = QSpinBox()
        self.chunk_size.setRange(1000, 1000000)
        self.chunk_size.setSingleStep(10000)
        self.chunk_size.setValue(100000)
        options_layout.addRow("Chunk Size:", self.chunk_size)

        self.file_format = QComboBox()
        self.file_format.addItems(['parquet', 'feather', 'csv', 'hdf5'])
        options_layout.addRow("Output Format:", self.file_format)

        main_layout.addWidget(options_group)

        # Process Button
        self.process_button = QPushButton('Process Data')
        self.process_button.clicked.connect(self.process_data)
        main_layout.addWidget(self.process_button)

        # Progress Bar
        self.progress_bar = QProgressBar()
        main_layout.addWidget(self.progress_bar)

        # Log Output
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        main_layout.addWidget(self.log_text)

    def select_input(self):
        self.input_dir = QFileDialog.getExistingDirectory(
            self, "Select Input Directory")
        self.log_text.append(f"Input directory: {self.input_dir}")

    def select_output(self):
        self.output_file, _ = QFileDialog.getSaveFileName(
            self, "Select Output File", "", "All Files (*)")
        self.log_text.append(f"Output file: {self.output_file}")

    def process_data(self):
        if not hasattr(self, 'input_dir') or not hasattr(self, 'output_file'):
            QMessageBox.warning(self, "Input/Output Not Set",
                                "Please select both input directory and output file before processing.")
            return

        self.process_button.setEnabled(False)
        self.progress_bar.setValue(0)
        self.log_text.clear()

        self.thread = DataProcessorThread(
            self.input_dir,
            self.output_file,
            self.use_sample_checkbox.isChecked(),
            self.sample_frac.value(),
            self.n_workers.value(),
            self.file_format.currentText(),
            self.chunk_size.value()
        )
        self.thread.progress.connect(self.update_progress)
        self.thread.log.connect(self.log)
        self.thread.finished.connect(self.process_finished)
        self.thread.start()

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def log(self, message):
        self.log_text.append(message)

    def process_finished(self):
        self.process_button.setEnabled(True)
        QMessageBox.information(
            self, "Processing Complete", "Data processing has finished.")


def run_gui():
    app = QApplication(sys.argv)
    main_window = MainWindow()
    main_window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    run_gui()
