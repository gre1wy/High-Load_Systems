import time
from concurrent.futures import ThreadPoolExecutor
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

KEYSPACE = 'ks_rf3'
TABLE = 'likes_counter'
ROW_ID = 1
NUM_CLIENTS = 10
INCREMENTS = 10000
EXPECTED_TOTAL = NUM_CLIENTS * INCREMENTS

cluster = Cluster(['cassandra-1'], port=9042)
session = cluster.connect()

def setup_infrastructure():
    """Створює Keyspace та Таблицю автоматично"""
    print("--- 1. Налаштування інфраструктури ---")

    create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{
        'class': 'SimpleStrategy',
        'replication_factor': 3
    }};
    """
    session.execute(create_keyspace_query)
    print(f"Keyspace '{KEYSPACE}' перевірено/створено.")

    session.set_keyspace(KEYSPACE)

    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id int PRIMARY KEY,
        likes_count counter
    );
    """
    session.execute(create_table_query)
    print(f"Таблиця '{TABLE}' перевірена/створена.")

def reset_counter():
    session.execute(f"TRUNCATE {TABLE}")
    print(f"Лічильник скинуто.")

def get_current_count():
    query = SimpleStatement(f"SELECT likes_count FROM {TABLE} WHERE id={ROW_ID}", consistency_level=ConsistencyLevel.QUORUM)
    result = session.execute(query).one()
    return result.likes_count if result else 0

def client_task(client_id, consist_level):
    query = f"UPDATE {TABLE} SET likes_count = likes_count + 1 WHERE id = {ROW_ID}"
    stmt = SimpleStatement(query, consistency_level=consist_level)
    
    for _ in range(INCREMENTS):
        try:
            session.execute(stmt)
        except Exception as e:
            print(f"Client {client_id} Error: {e}")

def run_test(name, consist_level):
    print(f"\nЗАПУСК ТЕСТУ: {name}")
    
    reset_counter()
    start = time.time()
    
    with ThreadPoolExecutor(max_workers=NUM_CLIENTS) as executor:
        futures = [executor.submit(client_task, i, consist_level) for i in range(NUM_CLIENTS)]
        for f in futures:
            f.result() 
            
    duration = time.time() - start
    total = get_current_count()
    
    print(f"Час виконання: {duration:.2f} сек")
    print(f"Очікувано: {EXPECTED_TOTAL}")
    print(f"Отримано:  {total}")
    
    if total == EXPECTED_TOTAL:
        print("РЕЗУЛЬТАТ: цілісність збережено")
    else:
        diff = EXPECTED_TOTAL - total
        loss = (diff / EXPECTED_TOTAL) * 100
        print(f"РЕЗУЛЬТАТ: втрата {diff} записів ({loss:.2f}%)")

if __name__ == "__main__":

    setup_infrastructure()
    
    run_test("CONSISTENCY LEVEL ONE", ConsistencyLevel.ONE)
    
    time.sleep(3)
    
    run_test("CONSISTENCY LEVEL QUORUM", ConsistencyLevel.QUORUM)

    cluster.shutdown()