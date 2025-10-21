# counter_runner.py
import os, time, threading
import psycopg2
from psycopg2 import errors
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE
import random

# --- DB CONFIG ---
DB_CFG = dict(
    host=os.environ.get("DB_HOST", "localhost"),
    port=int(os.environ.get("DB_PORT", "5432")),
    dbname=os.environ.get("DB_NAME", "labdb"),
    user=os.environ.get("DB_USER", "user"),
    password=os.environ.get("DB_PASS", "pass"),
)

# --- DB INIT ---
def init_database():
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS user_counter (
                user_id  INTEGER PRIMARY KEY,
                counter  INTEGER NOT NULL,
                version  INTEGER NOT NULL
            );
        """)
        for uid in range(1, 7):
            cur.execute(f"""
                INSERT INTO user_counter (user_id, counter, version)
                VALUES ({uid}, 0, 0)
                ON CONFLICT (user_id) DO NOTHING;
            """)
    conn.close()
    print("Table 'user_counter' is ready.\n")

def new_conn(serializable=False):
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = False
    if serializable:
        conn.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
    return conn

def reset_all():
    with new_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE user_counter SET counter=0, version=0;")
        conn.commit()
    print("All counters have been reset.\n")

# --- WORKERS ---
def lost_update_worker(iters, user_id):
    conn = new_conn()
    with conn.cursor() as cur:
        for _ in range(iters):
            cur.execute(f"SELECT counter FROM user_counter WHERE user_id = {user_id};")
            (c,) = cur.fetchone()
            c += 1
            cur.execute(f"UPDATE user_counter SET counter = {c} WHERE user_id = {user_id};")
            conn.commit()
    conn.close()
    
def serializable_naive_worker(iters, user_id):
    conn = new_conn(serializable=True)
    mistakes = 0
    with conn.cursor() as cur:
        for _ in range(iters):
            try:
                cur.execute(f"SELECT counter FROM user_counter WHERE user_id = {user_id};")
                (c,) = cur.fetchone()
                c += 1
                cur.execute(f"UPDATE user_counter SET counter = {c} WHERE user_id = {user_id};")
                conn.commit()
            except errors.SerializationFailure:
                conn.rollback()
                mistakes += 1
                continue

    if mistakes > 0:
        print(f"Lost updates: {mistakes}")
                
    conn.close()

def serializable_worker(iters, user_id):
    conn = new_conn(serializable=True)
    with conn.cursor() as cur:
        for _ in range(iters):
            while True:
                try:
                    cur.execute(f"SELECT counter FROM user_counter WHERE user_id = {user_id};")
                    (c,) = cur.fetchone()
                    c += 1
                    cur.execute(f"UPDATE user_counter SET counter = {c} WHERE user_id = {user_id};")
                    conn.commit()
                    break
                except errors.SerializationFailure:
                    conn.rollback()
                    time.sleep(0.001)
    conn.close()

def inplace_worker(iters, user_id):
    conn = new_conn()
    with conn.cursor() as cur:
        for _ in range(iters):
            cur.execute(f"UPDATE user_counter SET counter = counter + 1 WHERE user_id = {user_id};")
            conn.commit()
    conn.close()

def rowlock_worker(iters, user_id):
    conn = new_conn()
    with conn.cursor() as cur:
        for _ in range(iters):
            cur.execute(f"SELECT counter FROM user_counter WHERE user_id = {user_id} FOR UPDATE;")
            (c,) = cur.fetchone()
            c += 1
            cur.execute(f"UPDATE user_counter SET counter = {c} WHERE user_id = {user_id};")
            conn.commit()
    conn.close()

def optimistic_worker(iters, user_id):
    conn = new_conn()
    with conn.cursor() as cur:
        for _ in range(iters):
            while True:
                cur.execute(f"SELECT counter, version FROM user_counter WHERE user_id = {user_id};")
                c, v = cur.fetchone()
                c += 1
                cur.execute(f"""
                    UPDATE user_counter 
                    SET counter = {c}, version = {v + 1} 
                    WHERE user_id = {user_id} AND version = {v};
                """)
                updated = cur.rowcount
                conn.commit()
                if updated > 0:
                    break
    conn.close()

MODES = {
    1: ("Lost Update", lost_update_worker),
    2: ("Serializable (naive)", serializable_naive_worker),
    3: ("Serializable (safe)", serializable_worker),
    4: ("In-place", inplace_worker),
    5: ("Row Lock", rowlock_worker),
    6: ("Optimistic", optimistic_worker)
}

# --- MAIN ---
def run_mode(name, func, user_id, threads, iters):
    print(f"Running test: {name} (user_id={user_id})")
    total_ops = threads * iters
    start = time.perf_counter()
    threads_list = [threading.Thread(target=func, args=(iters, user_id)) for _ in range(threads)]
    for t in threads_list: 
        t.start()
    for t in threads_list: 
        t.join()
    dur = time.perf_counter() - start
    print(f"{name} finished in {dur:.2f} seconds | {total_ops/dur:.0f} ops/s\n")

def read_all():
    with new_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM user_counter ORDER BY user_id;")
            rows = cur.fetchall()
    print("Final table:")
    print("user_id | counter | version")
    print("----------------------------")
    for r in rows:
        print(f"{r[0]:7} | {r[1]:7} | {r[2]:7}")
    print()

def main():
    init_database()
    reset_all()

    THREADS = 10
    ITERS = 10000

    for uid, (name, func) in MODES.items():
        run_mode(name, func, uid, THREADS, ITERS)

    read_all()
    print("All tests completed.")

if __name__ == "__main__":
    main()
