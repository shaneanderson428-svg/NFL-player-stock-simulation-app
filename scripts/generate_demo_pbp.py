#!/usr/bin/env python3
import csv
import random
from datetime import datetime

random.seed(42)

OUT = "data/external/play_by_play_demo.csv"
N = 50000
passers = [
    "Patrick Mahomes",
    "Josh Allen",
    "Joe Burrow",
    "Justin Herbert",
    "Trevor Lawrence",
    "Jalen Hurts",
    "Aaron Rodgers",
    "Kirk Cousins",
    "Lamar Jackson",
    "Tua Tagovailoa",
    "Kenny Pickett",
]

start = datetime(2025, 9, 1)

with open(OUT, "w", newline="") as f:
    writer = csv.writer(f)
    # minimal header with relevant cols
    writer.writerow(["game_id", "play_id", "passer_player_name", "epa", "cpoe", "desc"])
    for i in range(N):
        game = 1000 + (i % 200)
        play = i
        passer = random.choice(passers)
        # epa centered per passer to simulate differences
        base = 0.5 * (passers.index(passer) % 5)
        epa = round(random.gauss(base, 1.2), 3)
        cpoe = round(random.gauss(0.0, 5.0), 2)
        writer.writerow([game, play, passer, epa, cpoe, "demo play"])

print("Wrote", OUT)
