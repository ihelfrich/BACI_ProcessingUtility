from setuptools import setup, find_packages

setup(
    name="data-analyzer-tool",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'dask',
        'PyQt5',
        'joblib'
    ],
    entry_points={
        'console_scripts': [
            'data-analyzer=src.gui:run_gui',
        ],
    },
    author="Ian Helfrich",
    author_email="ianhelfrich@outlook.com",
    description="A tool for analyzing and processing BACI international trade data from CEPII, creating samples, merging original data with country and product code data, and creating stratified samples for faster analysis",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    url="https://github.com/ihelfrich/baci_data-analyzer-tool",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
