import pandas as pd
import dask.dataframe as dd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.feather as feather
import numpy as np
import logging
import glob
import os
import json
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
import h5py

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, input_dir, output_file, use_sample=True, sample_frac=0.01, n_workers=None, file_format='parquet', chunk_size=100000):
        self.input_dir = input_dir
        self.output_file = output_file
        self.use_sample = use_sample
        self.sample_frac = sample_frac
        self.n_workers = n_workers or os.cpu_count()
        self.file_format = file_format
        self.chunk_size = chunk_size
        self.main_files = []
        self.auxiliary_files = {}
        self.auxiliary_dfs = {}

    def analyze_files(self):
        try:
            csv_files = glob.glob(f"{self.input_dir}/*.csv")
            logger.info(
                f"Found {len(csv_files)} CSV files in the input directory.")

            self.main_files = [f for f in csv_files if 'BACI_HS' in f]
            logger.info(f"Identified {len(self.main_files)} main BACI files.")

            self.auxiliary_files = {
                'country_codes': next((f for f in csv_files if 'country_codes' in f), None),
                'product_codes': next((f for f in csv_files if 'product_codes' in f), None)
            }

            if not self.auxiliary_files['country_codes'] or not self.auxiliary_files['product_codes']:
                raise FileNotFoundError(
                    "Could not find country_codes or product_codes file.")

            for aux_name, aux_file in self.auxiliary_files.items():
                logger.info(f"Processing auxiliary file: {aux_name}")
                self.auxiliary_dfs[aux_name] = pd.read_csv(aux_file)

                if aux_name == 'country_codes':
                    self.auxiliary_dfs[aux_name]['country_code'] = self.auxiliary_dfs[aux_name]['country_code'].astype(
                        int)
                elif aux_name == 'product_codes':
                    self.auxiliary_dfs[aux_name]['code'] = self.auxiliary_dfs[aux_name]['code'].astype(
                        str)

            logger.info("File analysis completed successfully.")
        except Exception as e:
            logger.error(f"Error in analyze_files: {str(e)}")
            raise

    def process_chunk(self, chunk):
        try:
            # Merge with country codes for exporter
            chunk = pd.merge(chunk, self.auxiliary_dfs['country_codes'],
                             left_on='i', right_on='country_code', how='left')
            chunk = chunk.rename(columns={
                'country_name': 'exporter_name',
                'country_iso2': 'exporter_iso2',
                'country_iso3': 'exporter_iso3'
            })

            # Merge with country codes for importer
            chunk = pd.merge(chunk, self.auxiliary_dfs['country_codes'],
                             left_on='j', right_on='country_code', how='left',
                             suffixes=('', '_importer'))
            chunk = chunk.rename(columns={
                'country_name': 'importer_name',
                'country_iso2': 'importer_iso2',
                'country_iso3': 'importer_iso3'
            })

            # Convert 'k' to string for merging with product codes
            chunk['k'] = chunk['k'].astype(str)

            # Merge with product codes
            chunk = pd.merge(chunk, self.auxiliary_dfs['product_codes'],
                             left_on='k', right_on='code', how='left')

            return chunk
        except Exception as e:
            logger.error(f"Error in process_chunk: {str(e)}")
            raise

    def stratified_sample(self, df):
        if self.use_sample:
            return df.groupby(['t', 'i', 'j'], group_keys=False).apply(lambda x: x.sample(frac=self.sample_frac) if len(x) > 0 else x)
        else:
            return df

    def process_file(self, file):
        try:
            dtypes = {
                'q': 'float64',
                'v': 'float64',
                'i': 'int64',
                'j': 'int64',
                'k': 'object',
                't': 'int64'
            }

            chunks = pd.read_csv(file, dtype=dtypes, na_values=[
                                 'NA', '           NA'], chunksize=self.chunk_size)
            processed_chunks = []

            for chunk in chunks:
                chunk = chunk.fillna({col: 0 for col in ['q', 'v']})
                processed_chunk = self.process_chunk(chunk)
                sampled_chunk = self.stratified_sample(processed_chunk)
                processed_chunks.append(sampled_chunk)

            return pd.concat(processed_chunks, ignore_index=True)
        except Exception as e:
            logger.error(f"Error processing file {file}: {str(e)}")
            raise

    def save_data(self, df, file_format):
        base_name = os.path.splitext(self.output_file)[0]
        if file_format == 'parquet':
            pq.write_table(pa.Table.from_pandas(df), f"{base_name}.parquet")
        elif file_format == 'feather':
            feather.write_feather(df, f"{base_name}.feather")
        elif file_format == 'csv':
            df.to_csv(f"{base_name}.csv", index=False)
        elif file_format == 'hdf5':
            df.to_hdf(f"{base_name}.h5", key='data', mode='w')
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

    def process_data(self, progress_callback=None, log_callback=None):
        try:
            self.analyze_files()

            with ProcessPoolExecutor(max_workers=self.n_workers) as executor:
                futures = [executor.submit(self.process_file, file)
                           for file in self.main_files]
                processed_data = []

                for i, future in enumerate(tqdm(futures, total=len(self.main_files), desc="Processing files")):
                    try:
                        result = future.result()
                        processed_data.append(result)

                        if progress_callback:
                            progress = int(
                                (i + 1) / len(self.main_files) * 100)
                            progress_callback(progress)

                        if log_callback:
                            log_callback(f"Processed file {
                                         i + 1} of {len(self.main_files)}")
                    except Exception as e:
                        logger.error(f"Error processing file {i}: {str(e)}")

            if not processed_data:
                raise ValueError("No data was successfully processed")

            final_data = pd.concat(processed_data, ignore_index=True)

            # Generate summary statistics
            summary = self.generate_summary(final_data)

            # Save processed data and summary
            self.save_data(final_data, self.file_format)
            summary.to_csv(os.path.splitext(self.output_file)
                           [0] + '_summary.csv', index=False)

            # Save processing metadata
            metadata = {
                "input_directory": self.input_dir,
                "output_file": self.output_file,
                "file_format": self.file_format,
                "use_sample": self.use_sample,
                "sample_fraction": self.sample_frac if self.use_sample else None,
                "number_of_workers": self.n_workers,
                "chunk_size": self.chunk_size,
                "total_rows_processed": len(final_data),
                "total_files_processed": len(self.main_files)
            }
            with open(os.path.splitext(self.output_file)[0] + '_metadata.json', 'w') as f:
                json.dump(metadata, f, indent=2)

            if log_callback:
                log_callback(f"Data processing complete. Output saved to {
                             self.output_file}")
                log_callback(f"Summary statistics saved to {
                             os.path.splitext(self.output_file)[0] + '_summary.csv'}")
                log_callback(f"Processing metadata saved to {
                             os.path.splitext(self.output_file)[0] + '_metadata.json'}")
        except Exception as e:
            logger.error(f"Error in process_data: {str(e)}")
            if log_callback:
                log_callback(f"Error: {str(e)}")
            raise

    def generate_summary(self, df):
        try:
            # Top 10 trading partners by total trade value
            top_partners = df.groupby('exporter_name')[
                'v'].sum().nlargest(10).reset_index()
            top_partners.columns = ['Country', 'Total Trade Value']

            # Top 10 traded products
            top_products = df.groupby(['k', 'description'])[
                'v'].sum().nlargest(10).reset_index()
            top_products.columns = ['Product Code',
                                    'Product Description', 'Total Trade Value']

            # Time series of total trade volume
            time_series = df.groupby('t')['v'].sum().reset_index()
            time_series.columns = ['Year', 'Total Trade Volume']

            # Additional summary statistics
            total_trade = df['v'].sum()
            unique_countries = df['i'].nunique() + df['j'].nunique()
            unique_products = df['k'].nunique()

            summary_stats = pd.DataFrame({
                'Metric': ['Total Trade Value', 'Unique Countries', 'Unique Products'],
                'Value': [total_trade, unique_countries, unique_products]
            })

            return pd.concat([top_partners, top_products, time_series, summary_stats], axis=1)
        except Exception as e:
            logger.error(f"Error in generate_summary: {str(e)}")
            raise
