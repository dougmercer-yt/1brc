# 1brc Python solutions

This repository contains a few solutions to the 1brc implemented in Python.

## Credit to

* [gunnarmorling](https://github.com/gunnarmorling/1brc)
* [ifnesi](https://github.com/ifnesi/1brc)
* [booty](https://github.com/booty/ruby-1-billion)
* [dannyvankooten](https://www.dannyvankooten.com/blog/2024/1brc/)

## Kudos to community members

* [@WouterLVV](https://github.com/WouterLVV) for [finding a 0.5s speed up by using try/except instead of if/else in `process_line` 🔥](src/community/doug_booty4_wouter.py)
* [@adamfarquhar](https://github.com/adamfarquhar) for [implementing a chunk-based solution that is nearly as fast as mmap, and also showing that we don't need to throw away good software design for the sake of speed 🚀](src/community/farquhar_v6.py)

## Generating data

Follow the instructions for generating data in the [original 1brc repository](https://github.com/gunnarmorling/1brc?tab=readme-ov-file#running-the-challenge)

Copy or symlink them to `data/measurements.txt`.

## Evaluate the solutions

Run

```bash
bash eval.sh | python calc_stats.py
```
