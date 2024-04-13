import sys

def calculate_average_times(input_lines):
    command_times = {}
    for line in input_lines:
        if line.startswith("Running"):
            current_command = " ".join(line.split()[1:])
            command_times[current_command] = []
        elif line.strip():
            minutes, seconds = line[:-1].split('m')
            total_seconds = int(minutes) * 60 + float(seconds)
            command_times[current_command].append(total_seconds)

    average_times = {}
    for command, times in command_times.items():
        if len(times) > 2:
            times.sort()
            average = sum(times[1:-1]) / (len(times) - 2)
            average_times[command] = average

    # Sort by average time in increasing order
    sorted_averages = sorted(average_times.items(), key=lambda item: item[1])
    return sorted_averages

def main():
    input_data = sys.stdin.read()
    averages = calculate_average_times(input_data.splitlines())

    for command, avg_time in averages:
        print(f"{command}: {avg_time:.3f} seconds")

if __name__ == "__main__":
    main()
