import socket
import json
import urllib.request
from bs4 import BeautifulSoup
import concurrent.futures

# 服务器的IP和端口
SERVER_IP = '127.0.0.1'
SERVER_PORT = 8080

# 要连接的服务器
server_address = (SERVER_IP, SERVER_PORT)

# 客户端socket
client_socket = None

# 连接到服务器
def connect_to_server():
    global client_socket
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(server_address)
    print('Connected to server.')

# 接收服务器发送的URL
def receive_url():
    data = client_socket.recv(1024).decode()
    return data

# 获取URL的页面大小
def get_page_size(url):
    try:
        response = urllib.request.urlopen(url)
        html = response.read()
        return len(html)
    except:
        return 0

# 解析网页并返回URL列表
def parse_webpage(url):
    urls = set()
    try:
        response = urllib.request.urlopen(url)
        html = response.read()
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            href = link.get('href')
            if href.startswith('http'):
                urls.add(href)
    except:
        pass
    # print(urls)
    return urls

# 处理URL的线程函数
def process_url(url, executor):
    global depth

    # 解析网页中的URL并返回给服务器
    urls = parse_webpage(url)
    for new_url in urls:
        if new_url != url:
            try:
                client_socket.send(json.dumps({'url': new_url, 'sizes': None}).encode())
            except:
                print("发送失败")
            if depth < 3:
                executor.submit(process_url, new_url, executor)
    depth += 1

# 启动客户端程序
def start_client():
    connect_to_server()

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        while True:
            url = receive_url()
            sizes = get_page_size(url)
            # 返回URL及其页面大小
            client_socket.send(json.dumps({'url': url, 'sizes': sizes}).encode())

            depth = 0
            executor.submit(process_url, url, executor)

try:
    # 启动客户端
    start_client()
except KeyboardInterrupt:
    print("Client stopped.")