import polars as pl

pl.Config.set_streaming_chunk_size(8_000_000)

df = (
    pl.scan_csv(
        "data/measurements.txt",
        separator=";",
        has_header=False,
        new_columns=["city", "value"],
    )
    .group_by("city")
    .agg(
        pl.min("value").alias("min"),
        pl.mean("value").alias("mean"),
        pl.max("value").alias("max"),
    )
    .sort("city")
    .collect(streaming=True)
)

print(
    "{",
    ", ".join(
        f"{data[0]}={data[1]:.1f}/{data[2]:.1f}/{data[3]:.1f}"
        for data in df.iter_rows()
    ),
    "}",
    sep="",
)
