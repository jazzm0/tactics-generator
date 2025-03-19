import csv
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

import chess
import chess.engine
from tqdm import tqdm

from path import csv_file_path


def validate_move(engine_path, fen, moves):
    engine = chess.engine.SimpleEngine.popen_uci(engine_path)
    engine.configure({"Skill Level": 20})

    board = chess.Board(fen)
    board.push_uci(moves[0])  # Make the first move to set up the position for the second move
    result = engine.play(board, chess.engine.Limit(time=2.0))  # Set a time limit for the engine
    stockfish_move = result.move
    info = engine.analyse(board, chess.engine.Limit(time=2.0))
    score = info['score'].relative.score(mate_score=10000)  # Use a large number for mate score

    if score is None:
        winning_chance = None
    elif score > 0:
        winning_chance = 1 / (1 + 10 ** (-score / 400))
    else:
        winning_chance = 1 - 1 / (1 + 10 ** (score / 400))
    # Use a large number for mate score

    engine.quit()
    return stockfish_move.uci(), moves[1], winning_chance > 0.9


def validate_and_store_moves(csv_file_path, engine_path, sqlite_db_path, lowest_rating, highest_rating, max_puzzles):
    incorrect_puzzle_ids = set()

    # Connect to SQLite database (or create it)
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    with open(csv_file_path, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames

        # Define column types
        column_types = {header: 'TEXT' for header in headers}
        for col in ['Rating', 'RatingDeviation', 'Popularity']:
            if col in column_types:
                column_types[col] = 'INTEGER'

        # Create a table with the same name as the CSV file (without extension)
        table_name = csv_file_path.split('/')[-1].split('.')[0]
        columns = ', '.join([f'"{header}" {column_types[header]}' for header in headers])
        cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns})')

        tasks = []
        processed_count = 0
        with ThreadPoolExecutor() as executor, tqdm(total=max_puzzles) as pbar:
            for row in reader:

                if (not lowest_rating <= int(row['Rating']) <= highest_rating or int(row['Popularity']) < 90) and int(
                        row['NbPlays']) < 1000:
                    continue
                fen = row['FEN']
                moves = row['Moves'].split()
                if len(moves) < 2:
                    print(f"Puzzle ID {row['PuzzleId']}: Not enough moves to validate")
                    continue
                tasks.append(executor.submit(validate_move, engine_path, fen, moves))

                # Process tasks in batches of 10
                if len(tasks) == 10:
                    for task in as_completed(tasks):
                        stockfish_move, correct_move, winning = task.result()
                        if stockfish_move == correct_move and winning:
                            placeholders = ', '.join(['?' for _ in headers])
                            cursor.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})',
                                           [row[header] for header in headers])
                            pbar.update(1)
                            processed_count += 1
                            if processed_count >= max_puzzles:
                                break
                        else:
                            incorrect_puzzle_ids.add(row['PuzzleId'])

                    tasks = []

            # Process any remaining tasks
            for task in as_completed(tasks):
                if processed_count >= max_puzzles:
                    break
                stockfish_move, correct_move, winning = task.result()
                if stockfish_move == correct_move and winning:
                    placeholders = ', '.join(['?' for _ in headers])
                    cursor.execute(f'INSERT INTO "{table_name}" VALUES ({placeholders})',
                                   [row[header] for header in headers])
                    pbar.update(1)
                    processed_count += 1
                else:
                    incorrect_puzzle_ids.add(row['PuzzleId'])

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

    print("Incorrect or low scored Puzzle IDs:")
    for puzzle_id in incorrect_puzzle_ids:
        print(puzzle_id)


engine_path = '/opt/homebrew/Cellar/stockfish/17/bin/stockfish'
sqlite_db_path = 'validated_puzzles.db'
max_puzzles = 10000
validate_and_store_moves(csv_file_path, engine_path, sqlite_db_path, 2001, 3500, max_puzzles)
