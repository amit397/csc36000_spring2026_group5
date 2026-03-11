#!/usr/bin/env python3
"""
test_grpc_worker.py

Tests for the gRPC WorkerService implementation.
Run with: python -m pytest test_grpc_worker.py -v
Or standalone: python test_grpc_worker.py
"""

import subprocess
import sys
import time
import grpc

# Add parent directory to path for imports
sys.path.insert(0, ".")

import primes_pb2
import primes_pb2_grpc


def wait_for_server(address: str, timeout: float = 10.0) -> bool:
    """Wait for gRPC server to be available."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            channel = grpc.insecure_channel(address)
            grpc.channel_ready_future(channel).result(timeout=1)
            channel.close()
            return True
        except grpc.FutureTimeoutError:
            time.sleep(0.5)
    return False


class TestWorkerService:
    """Tests for WorkerService gRPC methods."""
    
    @staticmethod
    def get_stub(address: str = "localhost:9100") -> primes_pb2_grpc.WorkerServiceStub:
        """Get a stub connected to the worker."""
        channel = grpc.insecure_channel(address)
        return primes_pb2_grpc.WorkerServiceStub(channel)
    
    def test_health_check(self):
        """Test Health RPC returns healthy status."""
        stub = self.get_stub()
        response = stub.Health(primes_pb2.HealthRequest())
        
        assert response.ok, "Health check should return ok=True"
        assert response.status == "healthy", f"Expected 'healthy', got '{response.status}'"
        print("✓ Health check: PASSED")
    
    def test_compute_range_count_mode(self):
        """Test ComputeRange with COUNT mode for known range."""
        stub = self.get_stub()
        
        # Primes in [0, 100): 2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97 = 25 primes
        request = primes_pb2.ComputeRequest(
            low=0,
            high=100,
            mode=primes_pb2.COUNT,
            chunk=50,
            exec_mode=primes_pb2.SINGLE,
        )
        response = stub.ComputeRange(request)
        
        assert response.ok, f"ComputeRange failed: {response.error}"
        assert response.total_primes == 25, f"Expected 25 primes in [0,100), got {response.total_primes}"
        assert response.range_low == 0, f"Expected range_low=0, got {response.range_low}"
        assert response.range_high == 100, f"Expected range_high=100, got {response.range_high}"
        print(f"✓ ComputeRange COUNT mode: PASSED (25 primes in [0,100))")
    
    def test_compute_range_list_mode(self):
        """Test ComputeRange with LIST mode returns actual primes."""
        stub = self.get_stub()
        
        request = primes_pb2.ComputeRequest(
            low=0,
            high=30,
            mode=primes_pb2.LIST,
            chunk=100,
            exec_mode=primes_pb2.SINGLE,
            max_return_primes=100,
        )
        response = stub.ComputeRange(request)
        
        expected_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29]
        
        assert response.ok, f"ComputeRange failed: {response.error}"
        assert response.total_primes == 10, f"Expected 10 primes, got {response.total_primes}"
        assert list(response.primes) == expected_primes, f"Expected {expected_primes}, got {list(response.primes)}"
        print(f"✓ ComputeRange LIST mode: PASSED (primes: {list(response.primes)})")
    
    def test_compute_range_list_truncation(self):
        """Test that primes are truncated when exceeding max_return_primes."""
        stub = self.get_stub()
        
        request = primes_pb2.ComputeRequest(
            low=0,
            high=100,
            mode=primes_pb2.LIST,
            chunk=50,
            exec_mode=primes_pb2.SINGLE,
            max_return_primes=5,  # Only return first 5 primes
        )
        response = stub.ComputeRange(request)
        
        assert response.ok, f"ComputeRange failed: {response.error}"
        assert len(response.primes) == 5, f"Expected 5 primes returned, got {len(response.primes)}"
        assert response.primes_truncated, "Expected primes_truncated=True"
        assert response.total_primes == 25, f"Total should still be 25, got {response.total_primes}"
        print(f"✓ ComputeRange truncation: PASSED (5 primes returned, 25 total, truncated=True)")
    
    def test_compute_range_invalid_range(self):
        """Test that invalid range (high <= low) returns error."""
        stub = self.get_stub()
        
        request = primes_pb2.ComputeRequest(
            low=100,
            high=50,  # Invalid: high < low
            mode=primes_pb2.COUNT,
        )
        
        try:
            response = stub.ComputeRange(request)
            # If we get a response, check that it's an error
            assert not response.ok or response.error, "Expected error for invalid range"
            print(f"✓ Invalid range error handling: PASSED (error: {response.error})")
        except grpc.RpcError as e:
            # gRPC error is also acceptable
            assert e.code() == grpc.StatusCode.INVALID_ARGUMENT
            print(f"✓ Invalid range error handling: PASSED (gRPC error: {e.details()})")
    
    def test_compute_range_exec_modes(self):
        """Test different execution modes (SINGLE, THREADS, PROCESSES)."""
        stub = self.get_stub()
        
        for exec_mode, mode_name in [
            (primes_pb2.SINGLE, "SINGLE"),
            (primes_pb2.THREADS, "THREADS"),
            (primes_pb2.PROCESSES, "PROCESSES"),
        ]:
            request = primes_pb2.ComputeRequest(
                low=0,
                high=1000,
                mode=primes_pb2.COUNT,
                chunk=200,
                exec_mode=exec_mode,
                workers=2,
            )
            response = stub.ComputeRange(request)
            
            assert response.ok, f"ComputeRange {mode_name} failed: {response.error}"
            # Primes in [0, 1000) = 168
            assert response.total_primes == 168, f"Expected 168 primes in [0,1000), got {response.total_primes} with {mode_name}"
            print(f"✓ ComputeRange {mode_name} mode: PASSED (168 primes)")
    
    def test_large_range(self):
        """Test computation on a larger range."""
        stub = self.get_stub()
        
        # Primes in [0, 10000) = 1229
        request = primes_pb2.ComputeRequest(
            low=0,
            high=10000,
            mode=primes_pb2.COUNT,
            chunk=1000,
            exec_mode=primes_pb2.PROCESSES,
            workers=4,
        )
        response = stub.ComputeRange(request, timeout=60)
        
        assert response.ok, f"ComputeRange failed: {response.error}"
        assert response.total_primes == 1229, f"Expected 1229 primes in [0,10000), got {response.total_primes}"
        print(f"✓ Large range test: PASSED (1229 primes in [0,10000), elapsed={response.elapsed_seconds:.3f}s)")
            
 
 
    def test_no_worker(self, worker):
        """Test that Compute fails when no workers are available."""
        
        try:
            # Kill the main worker so coordinator has no nodes
            worker.kill()
            worker.wait(timeout=2)
            
            
            # Connect to the coordinator (primary on 9300)
            channel = grpc.insecure_channel("localhost:9300")
            stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
            
            # Try to compute with no workers available
            request = primes_pb2.ComputeRequest(low=0, high=100, mode=primes_pb2.COUNT)
            
            try:
                response = stub.Compute(request, timeout=5)
                # If we got a response, it should be an error
                if response.ok:
                    raise AssertionError("FAIL: Compute succeeded, but no workers should be available.")
                # Error response is acceptable
                print(f"✓ No worker error handling: PASSED (coordinator error: {response.error})")
            except grpc.RpcError as e:
                # RPC error is also acceptable (e.g., UNAVAILABLE)
                print(f"✓ No worker error handling: PASSED (gRPC error: {e.details()})")
            finally:
                channel.close()
  
        except subprocess.TimeoutExpired:
            raise AssertionError("FAIL: Could not kill worker process for no-worker test.")
        

    def test_primary_twoWorkers(self):
        """Integration test: Primary with 2 secondary workers."""
        primary = subprocess.Popen(
            [sys.executable, "primary_node.py", "--host", "127.0.0.1", "--port", "9300"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        w2 = subprocess.Popen([sys.executable, "secondary_node.py", "--primary", "127.0.0.1:9300", "--port", "9101", "--node-id", "LocalWorker1"])
        w3 = subprocess.Popen([sys.executable, "secondary_node.py", "--primary", "127.0.0.1:9300", "--port", "9102", "--node-id", "LocalWorker2"])
        
        time.sleep(2)  # Give them time to register
        
        try:
            #python primes_cli.py --exec distributed --primary 127.0.0.1:9300 --low 1 --high 5000000 --mode count --include-per-node --time
            cmd = [
                sys.executable, "primes_cli.py",
                "--exec", "distributed",
                "--primary", "127.0.0.1:9300",
                "--low", "1",
                "--high", "5000000",
                "--mode", "count",
                "--include-per-node",
                "--time"
            ]
            print("RUNNING PRIMARY WITH 2 WORKERS: ", " ".join(cmd))
            result = subprocess.run(cmd, capture_output=True, text=True)
            print("STDOUT:\n", result)
            # Expect the CLI to succeed and print a numeric total of primes
            assert result.returncode == 0, f"CLI failed: {result.stderr}"
            out = result.stdout.strip()
            assert out.isdigit(), f"Expected numeric total, got: {out}"
            total = int(out)
            assert total > 0, f"Expected >0 primes, got {total}"
            print("✓ Integration Test: PASSED")
            
        finally:
            # 4. Cleanup
            w2.kill()
            w3.kill()
            primary.kill()




def run_all_tests():
    """Run all tests manually (without pytest)."""
    print("=" * 60)
    print("gRPC WorkerService Tests")
    print("=" * 60)
    print("\nMake sure secondary_node.py is running on localhost:9100")
    print("Command: python secondary_node.py --port 9100\n")
    
   
    print("Started primary coordinator on localhost:9300")
    time.sleep(1)  # Give primary time to start

    w1 = subprocess.Popen([sys.executable, "secondary_node.py", "--primary", "localhost:9200", "--port", "9100"])
    print("Connected to worker at localhost:9100\n")
    time.sleep(2)  # Give worker time to register

    
    tests = TestWorkerService()
    failures = []
    
    test_methods = [
        ("test_health_check", tests.test_health_check),
        ("test_compute_range_count_mode", tests.test_compute_range_count_mode),
        ("test_compute_range_list_mode", tests.test_compute_range_list_mode),
        ("test_compute_range_list_truncation", tests.test_compute_range_list_truncation),
        ("test_compute_range_invalid_range", tests.test_compute_range_invalid_range),
        ("test_compute_range_exec_modes", tests.test_compute_range_exec_modes),
        ("test_large_range", tests.test_large_range),
        ("test_no_worker", tests.test_no_worker),
        ("test_primary_twoWorkers", tests.test_primary_twoWorkers),
    ]
    
    for name, method in test_methods:
        try:
            if name == "test_no_worker":
                method(worker=w1)
            else:
                method()
        except AssertionError as e:
            print(f"✗ {name}: FAILED - {e}")
            failures.append(name)
        except Exception as e:
            print(f"✗ {name}: ERROR - {e}")
            failures.append(name)
    
    print("\n" + "=" * 60)
    

    if failures:
        print(f"FAILED: {len(failures)}/{len(test_methods)} tests")
        for name in failures:
            print(f"  - {name}")
        return 1
    else:
        print(f"PASSED: {len(test_methods)}/{len(test_methods)} tests")
        return 0


if __name__ == "__main__":
    sys.exit(run_all_tests())
