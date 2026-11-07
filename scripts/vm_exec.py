"""通用 SSH 执行助手：连接 VM 运行命令。

凭据全部来自环境变量（不写入任何文件/代码）：
    VM_HOST=<虚拟机IP> VM_USER=<用户名> VM_PASS=<密码> \
        .venv/Scripts/python scripts/vm_exec.py "hostname && docker ps"

也支持上传/下载：
    ... scripts/vm_exec.py --put 本地路径 远程路径
    ... scripts/vm_exec.py --get 远程路径 本地路径
"""

from __future__ import annotations

import argparse
import os
import sys

import paramiko


def connect() -> paramiko.SSHClient:
    host = os.environ["VM_HOST"]
    user = os.environ["VM_USER"]
    password = os.environ["VM_PASS"]
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(host, username=user, password=password, timeout=15)
    return client


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", nargs="?", help="要执行的远程命令")
    ap.add_argument("--put", nargs=2, metavar=("LOCAL", "REMOTE"))
    ap.add_argument("--get", nargs=2, metavar=("REMOTE", "LOCAL"))
    ap.add_argument("--timeout", type=int, default=600, help="命令超时秒数")
    args = ap.parse_args()

    client = connect()
    try:
        if args.put or args.get:
            sftp = client.open_sftp()
            if args.put:
                sftp.put(args.put[0], args.put[1])
                print(f"[put] {args.put[0]} -> {args.put[1]}")
            if args.get:
                sftp.get(args.get[0], args.get[1])
                print(f"[get] {args.get[0]} -> {args.get[1]}")
            sftp.close()
            return 0

        if not args.command:
            ap.error("需要命令或 --put/--get")
        stdin, stdout, stderr = client.exec_command(args.command, timeout=args.timeout)
        out = stdout.read().decode("utf-8", "replace")
        err = stderr.read().decode("utf-8", "replace")
        code = stdout.channel.recv_exit_status()
        if out:
            print(out, end="")
        if err:
            print("[stderr]", err, end="", file=sys.stderr)
        print(f"[exit={code}]")
        return code
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
