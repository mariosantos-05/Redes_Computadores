import subprocess
import sys
import os
import json
import time

def main():
    print("==================================================")
    print("STARTING TEST ORCHESTRATOR FOR P2P CHAT")
    print("==================================================")

    root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    config_path = os.path.join(root_dir, "config.json")
    
    # 1. Read original config.json
    print(f"Reading original config from {config_path}...")
    with open(config_path, "r", encoding="utf-8") as f:
        original_config = json.load(f)

    # 2. Prepare test config
    test_config = original_config.copy()
    test_config["rdv_host"] = "127.0.0.1"
    test_config["rdv_port"] = 8085

    rdv_process = None
    tests = [
        "test/test_1_rendezvous.py",
        "test/test_2_handshake.py",
        "test/test_3_keepalive.py",
        "test/test_4_messaging.py",
        "test/test_5_broadcast.py",
        "test/test_6_termination.py",
        "test/test_7_reconnection.py",
        "test/test_8_conn_and_states.py",
        "test/test_9_cli.py",
        "test/test_10_comprehensive.py",
    ]
    results = {}

    try:
        # Patch config.json
        print("Patching config.json for local testing...")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(test_config, f, indent=2)

        # 3. Start Rendezvous Server subprocess
        print("Launching local Rendezvous server from pyp2p-rdv...")
        rdv_path = os.path.join(root_dir, "pyp2p-rdv", "src", "rendezvous")
        env = os.environ.copy()
        env["PYTHONPATH"] = rdv_path
        
        rdv_cmd = [
            sys.executable,
            os.path.join(rdv_path, "main.py"),
            "--host", "127.0.0.1",
            "--port", "8085",
            "--log-mode", "console"
        ]
        
        rdv_process = subprocess.Popen(
            rdv_cmd,
            env=env,
            cwd=root_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Give it a moment to bind and start listening
        print("Waiting for Rendezvous server to initialize...")
        time.sleep(2.0)
        
        # Check if the process died immediately
        if rdv_process.poll() is not None:
            _, stderr = rdv_process.communicate()
            raise RuntimeError(f"Rendezvous server failed to start: {stderr.decode()}")
            
        print("Rendezvous server is up and listening on 127.0.0.1:8085.")

        # 4. Run tests sequentially
        for test in tests:
            test_path = os.path.join(root_dir, test)
            print(f"\n--- Running {test} ---")
            
            # Start test process
            proc = subprocess.Popen(
                [sys.executable, test_path],
                cwd=root_dir,
                stdout=sys.stdout,
                stderr=sys.stderr
            )
            proc.wait()
            
            if proc.returncode == 0:
                results[test] = "PASSED"
                print(f"--> {test}: PASSED")
            else:
                results[test] = "FAILED"
                print(f"--> {test}: FAILED (exit code {proc.returncode})")

    except Exception as e:
        print(f"Error during execution: {e}")
        sys.exit(1)
        
    finally:
        # 5. Clean up subprocess
        if rdv_process:
            print("\nShutting down local Rendezvous server...")
            rdv_process.terminate()
            try:
                rdv_process.wait(timeout=5.0)
            except subprocess.TimeoutExpired:
                print("Force killing Rendezvous server...")
                rdv_process.kill()
                rdv_process.wait()

        # 6. Restore original config.json
        print("Restoring original config.json...")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(original_config, f, indent=2)

    # 7. Print summary
    print("\n==================================================")
    print("TEST SUITE SUMMARY")
    print("==================================================")
    all_passed = True
    for test, res in results.items():
        print(f"{test}: {res}")
        if res != "PASSED":
            all_passed = False
    print("==================================================")

    if all_passed and len(results) == len(tests):
        print("ALL TESTS PASSED SUCCESSFULLY!")
        sys.exit(0)
    else:
        print("SOME TESTS FAILED.")
        sys.exit(1)

if __name__ == "__main__":
    main()
