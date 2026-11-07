#!/usr/bin/env bash
# 从 VM 取回 pipeline 结果到 Windows（在 Windows 的 Git Bash 中运行）。
# 用法：VM_USER=<用户名> VM_IP=<虚拟机IP> bash scripts/fetch_outputs.sh
set -euo pipefail

VM_USER="${VM_USER:?请设置 VM_USER}"
VM_IP="${VM_IP:?请设置 VM_IP}"
REMOTE_DIR="${REMOTE_DIR:-~/course-taxi-demand/outputs}"

echo "== 从 ${VM_USER}@${VM_IP}:${REMOTE_DIR} 拉取 =="
scp -r "${VM_USER}@${VM_IP}:${REMOTE_DIR}" ./outputs_vm
echo "== 完成：结果在 ./outputs_vm/ =="
echo "看板读取该目录：streamlit run app.py -- --outputs outputs_vm"
