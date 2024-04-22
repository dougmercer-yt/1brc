# Specify the commands to run as a list
commands=(
    "pypy3 src/doug_booty3.py"
    "pypy3 src/doug_booty4.py"
    "pypy3 src/doug_booty4_alternate.py"
    "python src/duckdb_1brc.py"
    "python src/polars_1brc.py"
    "pypy3 src/community/farquhar_v6.py --workers 10 --chunk_size 536870912"
    "pypy3 src/community/doug_booty4_wouter.py"
    "pypy3 src/community/doug_booty4_no_gc.py"
)

for cmd in "${commands[@]}"; do
    echo "Running $cmd"
    TIMES=""
    for n in {1..5}; do
        TIMES+="$({ time $cmd > /dev/null; } 2>&1 | awk '/real/{print $2}')"
        TIMES+=$'\n'
        sleep 1
    done
    echo "$TIMES"
done
