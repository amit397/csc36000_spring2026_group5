# Setup Guide

## 1. Create and activate virtual environment
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

If you get a permissions error:
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

## 2. Install dependencies
```powershell
pip install -r requirements.txt
```

## 3. Generate gRPC stubs
```powershell
Get-ChildItem protos\*.proto | ForEach-Object { python -m grpc_tools.protoc -I protos --python_out=generated --grpc_python_out=generated $_.Name }
```

## 4. Start the cluster
```powershell
python scripts\run_cluster.py
```

## 5. Stop the cluster
```powershell
python scripts\stop_cluster.py
```

## 6. Run a single replica manually (for testing)
```powershell
python replica_admin.py --host 127.0.0.1 --port 50061
```

## 7. Run tests
```powershell
pytest -q
```

## Notes
- If a port is stuck, check with `netstat -ano | findstr "50061"` and kill with `taskkill /F /PID <pid>`
- Don't commit the `venv/` folder
