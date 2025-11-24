import time
import threading
from concurrent.futures import ThreadPoolExecutor
from pymongo import MongoClient, WriteConcern
from pymongo.errors import AutoReconnect, PyMongoError


URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"

TOTAL_CLIENTS = 10         
INCREMENTS_PER_CLIENT = 10000  
DOC_ID = "likes_counter"

def worker_task(client_id, wc_mode):
    
    client = MongoClient(URI)
    
    wc = WriteConcern(w=wc_mode, wtimeout=5000)
    
    db = client.get_database("lab4_db", write_concern=wc)
    collection = db.likes
    
    for i in range(INCREMENTS_PER_CLIENT):
        collection.find_one_and_update(
            {"_id": DOC_ID},
            {"$inc": {"count": 1}},
            upsert=True
        )

            
    client.close()

def run_experiment(wc_mode, description):
    print(f"\n{'='*60}")
    print(f"ЗАПУСК ТЕСТУ: {description}")
    print(f"Write Concern: {wc_mode}")
    print(f"{'='*60}")

    init_client = MongoClient(URI)
    init_coll = init_client.get_database("lab4_db", write_concern=WriteConcern("majority")).likes
    init_coll.delete_many({})
    init_coll.insert_one({"_id": DOC_ID, "count": 0})
    print("Лічильник скинуто до 0.")
    init_client.close()

    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=TOTAL_CLIENTS) as executor:
        futures = [executor.submit(worker_task, i, wc_mode) for i in range(TOTAL_CLIENTS)]
        
        for future in futures:
            future.result()

    duration = time.time() - start_time

    check_client = MongoClient(URI)
    final_doc = check_client.lab4_db.likes.find_one({"_id": DOC_ID})
    check_client.close()

    final_count = final_doc['count']
    expected = TOTAL_CLIENTS * INCREMENTS_PER_CLIENT

    print(f"\n--- РЕЗУЛЬТАТИ ({description}) ---")
    print(f"Час виконання: {duration:.2f} сек")
    print(f"Кінцеве значення: {final_count}")
    print(f"Очікувано: {expected}")
    
    if final_count == expected:
        print("УСПІХ: Дані цілісні.")
    else:
        print(f"ПОМИЛКА: Втрачено {expected - final_count} оновлень!")

if __name__ == "__main__":
    print("Виберіть тест (1-4):")
    print("1. WC=1 (Нормальний режим)")
    print("2. WC=majority (Нормальний режим)")
    print("3. WC=1 + Fail (Треба вручну вимкнути Primary!)")
    print("4. WC=majority + Fail (Треба вручну вимкнути Primary!)")
    
    choice = input("Ваш вибір: ")

    if choice == '1':
        run_experiment(1, "WriteConcern = 1 (Normal)")
    elif choice == '2':
        run_experiment("majority", "WriteConcern = Majority (Normal)")
    elif choice == '3':
        print("\n!!! УВАГА: Під час виконання скрипта виконайте 'docker stop <primary_node>' у іншому вікні !\n")
        time.sleep(3)
        run_experiment(1, "WriteConcern = 1 (With FAILURE)")
    elif choice == '4':
        print("\n!!! УВАГА: Під час виконання скрипта виконайте 'docker stop <primary_node>' у іншому вікні !\n")
        time.sleep(3)
        run_experiment("majority", "WriteConcern = Majority (With FAILURE)")