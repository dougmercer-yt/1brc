import polars as pl


def create_polars_df(filename):
    pl.Config.set_streaming_chunk_size(8_000_000)
    return (
        pl.scan_csv(
            filename,
            separator=";",
            has_header=False,
            new_columns=["station", "measure"],
        )
        .group_by(by="station")
        .agg(
            max=pl.col("measure").max(),
            min=pl.col("measure").min(),
            mean=pl.col("measure").mean(),
        )
        .sort("station")
        .collect(streaming=True)
    )


if __name__ == "__main__":
    df = create_polars_df("data/measurements.txt")
    print(df)
