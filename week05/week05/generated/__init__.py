"""Generated protobuf modules live here."""

from pathlib import Path
import sys

# grpc_tools generates absolute imports (e.g. `import direct_client_pb2`).
# Ensure this directory is on sys.path so sibling generated modules resolve.
_this_dir = str(Path(__file__).resolve().parent)
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)
