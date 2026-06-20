# run_app.py - 一键启动大数据分析Web看板的脚本
# 功能说明：
# 1. 环境自检：检查必备文件和端口占用情况。
# 2. 子进程异步管理：使用subprocess.Popen异步启动uvicorn服务。
# 3. 服务就绪检测与自动唤起浏览器：轮询健康检查接口，确认服务启动后打开浏览器访问首页。
# 4. 优雅终止捕获：通过捕获Ctrl+C信号来关闭所有子进程。

import os
import subprocess
import webbrowser
import requests
import socket
import signal
import time

# 全局常量定义
PORT = 8000
HEALTH_CHECK_URL = f"http://127.0.0.1:{PORT}/api/health"
MAX_WAIT_TIME = 30  # 服务启动超时时间（秒）
POLL_INTERVAL = 0.5  # 轮询间隔（秒）
REQUIRED_FILES = ["server.py", "frontend/index.html"]  # 必备文件列表

# 存储子进程对象
subprocesses = []


def check_required_files():
    """检查项目根目录下是否包含必需文件"""
    for file in REQUIRED_FILES:
        if not os.path.exists(file):
            print(f"错误：缺少必需文件 {file}，请检查！")
            exit(1)


def is_port_in_use(port: int) -> bool:
    """检查指定端口是否已被占用"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0


def start_uvicorn():
    """异步后台启动Uvicorn服务"""
    if is_port_in_use(PORT):
        print(f"错误：本地{PORT}端口已被占用，请释放该端口后再试。")
        exit(1)

    process = subprocess.Popen(
        ["uvicorn", "server:app", "--host", "127.0.0.1", "--port", str(PORT), "--reload"]
    )
    subprocesses.append(process)
    print("已启动Uvicorn服务...")


def wait_for_service_ready():
    """轮询健康检查接口，确认服务是否就绪"""
    start_time = time.time()
    while True:
        try:
            response = requests.get(HEALTH_CHECK_URL)
            if response.status_code == 200:
                print("服务已成功启动！")
                break
        except requests.RequestException:
            pass

        if time.time() - start_time > MAX_WAIT_TIME:
            print("错误：服务启动超时，请检查配置或日志信息。")
            stop_all_subprocesses()
            exit(1)

        time.sleep(POLL_INTERVAL)


def open_web_browser():
    """自动打开默认浏览器访问看板首页"""
    webbrowser.open(f"http://127.0.0.1")


def stop_all_subprocesses():
    """遍历并终止所有子进程"""
    for p in subprocesses:
        try:
            p.terminate()
            p.wait(timeout=5)  # 等待进程退出
        except Exception as e:
            print(f"警告：尝试关闭进程时发生异常: {e}")


def main():
    try:
        check_required_files()
        start_uvicorn()
        wait_for_service_ready()
        open_web_browser()
        print("按 Ctrl+C 停止服务...")
        signal.pause()  # 主线程阻塞等待中断信号
    except KeyboardInterrupt:
        print("\n正在停止服务...")
        stop_all_subprocesses()
        print("服务已安全关闭。")
    except Exception as e:
        print(f"未知错误: {e}")
        stop_all_subprocesses()


if __name__ == "__main__":
    main()