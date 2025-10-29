# Runner to execute the main analysis cell from the notebook with chunking enabled
import json
from pathlib import Path

nb_path = Path("nflfastR_analysis.ipynb")
nb = json.loads(nb_path.read_text())

# Extract code cells in order; we'll set parameters in a namespace and exec the main cell
code_cells = [c for c in nb.get("cells", []) if c.get("cell_type") == "code"]

# Find the parameters cell (assume it's the first code cell)
params_src = "".join(code_cells[0].get("source", []))
# Use the last code cell as the main analysis cell (robust to edits)
main_src = "".join(code_cells[-1].get("source", []))

# Prepare namespace with parameters adjusted
ns: dict[str, object] = {}
# Defaults from the parameters cell
exec(params_src, ns)
# Force chunking on for this run
ns["use_chunked_aggregation"] = True
# Optionally adjust chunksize
ns["chunksize"] = 200_000

# Execute main analysis cell in the namespace
print(
    "Running full-season analysis with chunking enabled (this may take several minutes)"
)
exec(main_src, ns)
print("Runner finished")
