import requests
import threading
import time
import json

URL = "http://localhost:8080/api/workflows"
HEADERS = {"Content-Type": "application/json"}
TOTAL_WORKFLOWS = 1000
THREADS = 50

payload = json.dumps({
    "name": "soak-workflow",
    "stages": [
        {"name": "stage-A", "dependsOn": []},
        {"name": "stage-B", "dependsOn": []},
        {"name": "stage-C", "dependsOn": []},
        {"name": "stage-D", "dependsOn": []}
    ]
})

success = 0
failed = 0
lock = threading.Lock()

def send_workflow():
    global success, failed
    try:
        r = requests.post(URL, headers=HEADERS, data=payload, timeout=10)
        with lock:
            if r.status_code == 200:
                success += 1
            else:
                failed += 1
    except Exception as e:
        with lock:
            failed += 1

print(f"Starting soak test: {TOTAL_WORKFLOWS} workflows, {THREADS} threads...")
start = time.time()

threads = []
batch_size = TOTAL_WORKFLOWS // THREADS
for _ in range(THREADS):
    for _ in range(batch_size):
        t = threading.Thread(target=send_workflow)
        threads.append(t)

for t in threads:
    t.start()
for t in threads:
    t.join()

elapsed = time.time() - start
total_stages = success * 4  # 4 stages per workflow
stages_per_min = (total_stages / elapsed) * 60

print(f"\n=== SOAK TEST RESULTS ===")
print(f"Duration:          {elapsed:.1f}s")
print(f"Workflows sent:    {success} success / {failed} failed")
print(f"Total stages:      {total_stages}")
print(f"Stage completions: {stages_per_min:.0f}/minute")
print(f"========================")