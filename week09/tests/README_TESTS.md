These tests are black-box tests for the week09 starter.

They assume you provide:

- `scripts/run_cluster.py`
- `scripts/stop_cluster.py`
- `scripts/start_shard.py <id>`
- `scripts/stop_shard.py <id>`
- the gRPC contracts in `protos/`
- student implementations in `student_impl/`

The application-specific tests run only for the selected application in
`student_impl/project_choice.py`.

The test suite also reads the declared sharding and isolation tradeoffs
from `student_impl/project_choice.py` and validates them against
observable behavior where possible.

You can also pass the selected application explicitly to pytest with:

```bash
pytest -q --application course_registration
pytest -q --application wallet
pytest -q --application inventory
```

The `--application` flag must match the value in `student_impl/project_choice.py`.

Run with:

```bash
pip install -r requirements.txt
pytest -q
```

Recommended student workflow:

```bash
python -m pip install -r requirements.txt
python -m pytest -q --application course_registration
```

Replace `course_registration` with the application your team selected.
