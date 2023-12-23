import json

import cx_Oracle
import threading
import time
import socket

# 开启服务器的IP和端口
SERVER_IP = '127.0.0.1'
SERVER_PORT = 8888

# 数据库连接配置
DB_USER = 'U_0701'
DB_PASSWORD = '自己去改'
DB_HOST = 'localhost'
DB_PORT = '1521'
DB_SID = 'orcl'


# 从数据库中获取URL并发送给客户端处理
def send_urls_to_clients():
    conn = cx_Oracle.connect(DB_USER, DB_PASSWORD, DB_HOST + ':' + DB_PORT + '/' + DB_SID)
    cursor = conn.cursor()

    while True:
        cursor.execute("SELECT url FROM url_pool WHERE status='waiting' OR sizes IS NULL")
        rows = cursor.fetchall()
        print('aaa' + str(rows))

        if len(rows) > 0:
            # 均分URL给所有已连接的客户端
            num_clients = len(clients)
            if num_clients > 0:
                urls_per_client = len(rows) // num_clients

                for i, client in enumerate(clients):
                    start_index = i * urls_per_client
                    end_index = (i + 1) * urls_per_client

                    if i == num_clients - 1:
                        # 最后一个客户端处理剩余的URL
                        url_batch = rows[start_index:]
                    else:
                        url_batch = rows[start_index:end_index]

                    for row in url_batch:
                        url = row[0]
                        client.send(url.encode())
        time.sleep(10)  # 每10秒运行一次


# 处理客户端返回的URL及其网页大小
def process_client_response(client, addr):
    while True:
        data_str = client.recv(1024).decode()
        try:
            data = json.loads(data_str)
        except:
            print("解析失败")
            print(data_str)
            continue
        conn = cx_Oracle.connect(DB_USER, DB_PASSWORD, DB_HOST + ':' + DB_PORT + '/' + DB_SID)
        cursor = conn.cursor()
        print(data_str)
        # 更新数据库URL大小
        if data['sizes'] is None:
            cursor.execute("insert into url_pool (url, status) values (:url, 'waiting')", {'url': data['url']})
        else:
            cursor.execute("UPDATE url_pool SET sizes=:sizes, status='completed' WHERE url=:url", {'sizes': data['sizes'], 'url': data['url']})
        conn.commit()

        conn.close()


# 启动服务器
def start_server():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((SERVER_IP, SERVER_PORT))
        server_socket.listen(5)

        while True:
            client, addr = server_socket.accept()

            # 启动新线程处理客户端请求
            client_thread = threading.Thread(target=process_client_response, args=(client, addr))
            client_thread.start()

            # 添加客户端到列表
            with lock:
                clients.append(client)
                print('New client connected:', addr)

# 启动发送URL的线程
send_urls_thread = threading.Thread(target=send_urls_to_clients)
send_urls_thread.start()
# 客户端连接列表
clients = []

# 锁，用于保护clients列表
lock = threading.Lock()

def main():
    sk = socket.socket()
    sk.bind(('localhost', 8080))
    sk.listen(5)
    # 设置初始网址
    # initial_url = 'http://vlab.csu.edu.cn/webTech/references.html'
    initial_url = 'https://www.chinaw3c.org/'
    conn = cx_Oracle.connect(DB_USER, DB_PASSWORD, DB_HOST + ':' + DB_PORT + '/' + DB_SID)
    cursor = conn.cursor()
    # 检查初始网址是否已存在于URL池中
    cursor.execute("SELECT url FROM url_pool WHERE url = :url", {'url': initial_url})
    # cursor.execute("SELECT * FROM url_pool")
    row = cursor.fetchone()
    # print(row)
    if row is None:
        cursor.execute("INSERT INTO url_pool (url, status) VALUES (:url, 'waiting')", {'url': initial_url})
        conn.commit()
    while True:
        client, addr = sk.accept()

        # 启动新线程处理客户端请求
        client_thread = threading.Thread(target=process_client_response, args=(client, addr))
        client_thread.start()

        # 添加客户端到列表
        with lock:
            clients.append(client)
            print('New client connected:', addr)

# 启动程序
if __name__ == '__main__':
    main()