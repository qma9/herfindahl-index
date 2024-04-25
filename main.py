from time import process_time

from pandas import read_csv


def main() -> None:

    df = read_csv("D&B/MGNTFIS3.I9Q5VOQI.DMI.1990.csv")

    print(f"\n{df.columns}\n")
    print(f"\n{df.head()}\n")


if __name__ == "__main__":

    start_time = process_time()

    main()

    end_time = process_time()

    print(f"\nTime: {end_time - start_time} seconds\n")
