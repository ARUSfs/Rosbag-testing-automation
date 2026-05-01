#!/usr/bin/env bash
set -eo pipefail

# 1. Cargar el entorno de ROS 2 (apagando la regla estricta temporalmente)
set +u
source /opt/ros/humble/setup.bash
set -u

# 2. Ir directamente a la carpeta donde está tu código C++
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MONITOR_DIR="$SCRIPT_DIR/ros2_monitor"

if [[ ! -d "$MONITOR_DIR" ]]; then
    echo "[ERROR] No se encuentra la carpeta $MONITOR_DIR" >&2
    exit 1
fi

cd "$MONITOR_DIR"

# 3. Compilar el paquete de forma aislada (creará install/ y build/ aquí dentro)
echo "[BUILD] Compilando nodo C++ localmente..."
colcon build --cmake-args -DCMAKE_BUILD_TYPE=Release --event-handlers console_direct+

# 4. Imprimir la ruta que necesita el script de Python para funcionar
echo "SETUP_BASH=$MONITOR_DIR/install/setup.bash"
