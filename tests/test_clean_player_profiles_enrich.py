import sys
from pathlib import Path
import pandas as pd

# ensure repo root is importable so we can import scripts.clean_player_profiles
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def test_enrich_from_advanced_json():
    """Ensure enrichment pulls position from data/advanced/4240603.json"""
    p = Path("data/advanced/4240603.json")
    assert p.exists(), "expected advanced player JSON to exist for this test"

    from scripts.clean_player_profiles import enrich_profiles

    df = pd.DataFrame(
        [
            {
                "espnId": "4240603",
                "player": "Justin Jefferson",
                "team": "",
                "position": "",
            }
        ]
    )
    out = enrich_profiles(df.copy())
    # position should be filled to 'WR' per the advanced JSON
    assert str(out.loc[0, "position"]).upper() == "WR"
