from csv_profiler.io import read_csv_rows
from csv_profiler.profile import profile
from csv_profiler.render import write_json, write_markdown


def main() -> None:
    rows = read_csv_rows("data/sample.csv")
    report = profile(rows)

    write_json(report, "outputs/report.json")
    write_markdown(report, "outputs/report.md")

    print("Day 2 report generated successfully.")


if __name__ == "__main__":
    main()
