import argparse

import pandas as pd


def preprocess(input_path: str, output_path: str):
    df = pd.read_csv(input_path)
    df = df.drop(['Name', 'Ticket', 'Cabin', 'PassengerId'], axis=1, errors='ignore')
    df['Age'] = df['Age'].fillna(df['Age'].median())
    df = pd.get_dummies(df, columns=['Sex'], drop_first=True)
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    preprocess(args.input, args.output)
