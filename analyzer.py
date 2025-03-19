import csv

from matplotlib import pyplot as plt

from path import csv_file_path


def plot_histogram(csv_file_path):
    ratings = []
    popularity = []
    plays = []

    # Read the CSV file and extract ratings
    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            ratings.append(int(row['Rating']))
            popularity.append(int(row['Popularity']))
            plays.append(int(row['NbPlays']))

    # # Plot the ratings using a histogram
    # plt.figure(figsize=(10, 6))
    # plt.hist(ratings, bins=50, alpha=0.75, edgecolor='black')
    # plt.title('Rating Distribution of Chess Puzzles')
    # plt.xlabel('Rating')
    # plt.ylabel('Frequency')
    # plt.show()
    #
    # plt.figure(figsize=(10, 6))
    # plt.hist(popularity, bins=50, alpha=0.75, edgecolor='black')
    # plt.title('Popularity Distribution of Chess Puzzles')
    # plt.xlabel('Popularity')
    # plt.ylabel('Frequency')
    # plt.show()

    plt.figure(figsize=(10, 6))
    plt.hist(plays, bins=50, alpha=0.75, edgecolor='black')
    plt.title('NbPlays Distribution of Chess Puzzles')
    plt.xlabel('NbPlays')
    plt.ylabel('Frequency')
    plt.show()


plot_histogram(csv_file_path)
