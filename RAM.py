def load_parquet_to_numpy_float32(filepath, columns=None):
    parquet_file = pq.ParquetFile(filepath)

    if columns is None:
        columns = parquet_file.schema.names

    num_rows = parquet_file.metadata.num_rows
    num_cols = len(columns)

    matrix = np.empty((num_rows, num_cols), dtype=np.float32)

    for i, col_name in tqdm(enumerate(columns)):
        col_data = parquet_file.read([col_name])
        matrix[:, i] = col_data[col_name].to_numpy()
        
        del col_data
        if i % 10 == 0:
            gc.collect()

    return matrix
