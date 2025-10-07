# Файл: load_test.py
import requests
import time
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://localhost:8080"
REQUESTS_PER_CLIENT = 10000

def reset_counter():
    """Скидає лічильник перед початком тесту (Вам потрібно буде реалізувати /reset)"""
    # Оскільки ми не реалізували /reset, вам доведеться
    # 1. Для In-Memory: перезапустити Go-сервер.
    # 2. Для DB: вручну виконати SQL: UPDATE counter_table SET value = 0 WHERE id = 1;
    print("!!! Перед запуском переконайтеся, що Go-сервер перезапущений або лічильник БД скинутий на 0 !!!")


def make_requests(client_id):
    """Функція, яка виконує 10000 запитів /inc, використовуючи Session."""
    
    # Створюємо сесію, яка буде повторно використовувати з'єднання
    with requests.Session() as session:
        for _ in range(REQUESTS_PER_CLIENT):
            try:
                # Використовуємо сесію замість прямого requests.get
                session.get(f"{BASE_URL}/inc")
            except requests.RequestException as e:
                print(f"Клієнт {client_id} помилка запиту: {e}")
                pass

def get_final_count():
    """Отримує кінцеве значення /count"""
    try:
        response = requests.get(f"{BASE_URL}/count", timeout=5)
        return int(response.text.strip())
    except Exception:
        return "ERROR"

def run_test(num_clients):
    """Запускає навантажувальний тест з N клієнтами"""
    print(f"\n--- Тест: {num_clients} Клієнт(а/ів) ---")
    
    start_time = time.time()
    
    # Використання ThreadPoolExecutor для паралельного виконання
    with ThreadPoolExecutor(max_workers=num_clients) as executor:
        futures = [executor.submit(make_requests, i) for i in range(num_clients)]
        
        # Очікування завершення всіх клієнтів
        for future in futures:
            future.result()

    end_time = time.time()
    total_time = end_time - start_time
    total_requests = num_clients * REQUESTS_PER_CLIENT
    
    # Розрахунок Throughput
    throughput = total_requests / total_time
    final_count = get_final_count()

    print(f"Очікуваний Count: {total_requests:,}")
    print(f"Фактичний Count: {final_count:,}")
    print(f"Сумарний Час: {total_time:.4f} с")
    print(f"Throughput: {throughput:,.2f} req/s")
    
    return total_time, throughput

if __name__ == "__main__":
    
    # reset_counter() # Виконуйте вручну, як описано вище
    
    client_loads = [1, 2, 5, 10]
    
    # Рекомендація: Тестуйте Частину I, потім Частину II
    
    results = {}
    for clients in client_loads:
        # Для коректності результатів, перезапускайте сервер між тестами
        # або скидайте лічильник БД.
        
        # Запуск тесту
        time_taken, throughput = run_test(clients)
        results[clients] = {'time': time_taken, 'throughput': throughput}
    print(results)