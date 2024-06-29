# BACI Data Analyzer Tool

## Created by Ian Helfrich, 2024

## Table of Contents
1. [Introduction](#introduction)
2. [Features](#features)
3. [Installation](#installation)
4. [Usage](#usage)
5. [File Formats](#file-formats)
6. [Processing Options](#processing-options)
7. [Output](#output)
8. [Technical Details](#technical-details)
9. [Troubleshooting](#troubleshooting)
10. [Contributing](#contributing)
11. [License](#license)

## Introduction

The BACI Data Analyzer Tool is a powerful and user-friendly application designed to process and analyze large-scale international trade data from the BACI (Base pour l'Analyse du Commerce International) database. Created by Ian Helfrich, this tool aims to simplify the handling of complex trade datasets, allowing researchers and analysts to efficiently process, sample, and extract insights from BACI data.

## Features

- Graphical User Interface (GUI) for easy interaction
- Support for processing multiple BACI CSV files
- Automatic merging with country and product code data
- Option to process full dataset or create stratified samples
- Parallel processing for improved performance
- Multiple output formats (Parquet, Feather, CSV, HDF5)
- Customizable processing parameters (chunk size, number of workers)
- Detailed logging and progress tracking
- Generation of summary statistics

## Installation

1. Clone the repository:
git clone https://github.com/ihelfrich/baci-data-analyzer.git
cd baci-data-analyzer

2. Create a virtual environment (optional but recommended):
python -m venv venv
source venv/bin/activate  # On Windows, use venv\Scripts\activate

3. Install the required dependencies:
pip install -r requirements.txt

4. Install the package in editable mode:
pip install -e .

## Usage

To run the BACI Data Analyzer Tool:
data-analyzer
Copy
This will launch the graphical user interface.

## File Formats

The tool supports the following input and output formats:

### Input
- CSV files from the BACI database
- Country codes CSV file
- Product codes CSV file

### Output
- Parquet
- Feather
- CSV
- HDF5

## Processing Options

- **Use Sample**: Option to process a sample of the data instead of the full dataset
- **Sample Fraction**: The fraction of data to sample (if sampling is enabled)
- **Number of Workers**: Number of parallel processes to use
- **Chunk Size**: Number of rows to process at a time (affects memory usage)
- **Output Format**: Choose between Parquet, Feather, CSV, or HDF5

## Output

The tool generates the following outputs:

1. Processed data file in the selected format
2. Summary statistics CSV file
3. Processing metadata JSON file

## Technical Details

### Main Components

1. `processor.py`: Contains the `DataProcessor` class responsible for data processing logic
2. `gui.py`: Implements the graphical user interface using PyQt5
3. `utils.py`: Contains utility functions for file analysis and data handling

### Key Libraries Used

- pandas: Data manipulation and analysis
- dask: Parallel computing
- pyarrow: Efficient data storage and retrieval
- PyQt5: GUI framework
- concurrent.futures: Parallel execution
- tqdm: Progress bar functionality

## Troubleshooting

If you encounter any issues:

1. Ensure all dependencies are correctly installed
2. Check the log output in the GUI for error messages
3. Verify that input files are in the correct format and location
4. Try processing with a smaller chunk size if you're experiencing memory issues

## Contributing

Contributions to the BACI Data Analyzer Tool are welcome. Please feel free to submit pull requests or create issues for bugs and feature requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

---

For any questions or support, please contact Ian Helfrich at [ianhelfrich@outlook.com].