from m1_data_pipeline_optimized import M1DataPipeline

if __name__ == "__main__":
        pipeline = M1DataPipeline(data_path="m1_final_clean.parquet")
        data = pipeline.extract()
        report = pipeline.transform(data)
        pipeline.load(report)
