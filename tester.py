#!/usr/bin/env python3
"""
ROSBAG AUTOMATION TESTING
Sistema automático de testeo de rosbags.
"""

import os
import sys
import time
import shutil
import signal
import subprocess
import logging
from datetime import datetime
from pathlib import Path

import yaml

from checkers import build_checkers


# ──────────────────────────────────────────────
#  Logging setup
# ──────────────────────────────────────────────
def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"tester_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logger = logging.getLogger("rosbag_tester")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


# ──────────────────────────────────────────────
#  Config loader
# ──────────────────────────────────────────────
def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r") as f:
        config = yaml.safe_load(f)

    required_keys = ["directories", "rosbag_play", "rosbag_launch", "testing"]
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required config section: '{key}'")

    return config


# ──────────────────────────────────────────────
#  BUILD — compilación automática del monitor C++
# ──────────────────────────────────────────────
def build_cpp_monitor(config: dict, logger: logging.Logger) -> str:
    """
    Ejecuta build_monitor.sh para compilar el paquete ros2_monitor.

    Parsea la línea "SETUP_BASH=..." de stdout para obtener la ruta
    al setup.bash del workspace, que se inyecta en el entorno del proceso.

    Retorna la ruta al setup.bash del workspace instalado.
    Lanza RuntimeError si la compilación falla.
    """
    build_cfg   = config.get("build", {})
    workspace   = build_cfg.get("ros2_workspace", str(Path.home() / "ros2_ws"))
    script_dir  = Path(__file__).parent.resolve()
    build_script = script_dir / "build_monitor.sh"

    if not build_script.exists():
        raise FileNotFoundError(
            f"Script de compilación no encontrado: {build_script}\n"
            "Asegúrate de que build_monitor.sh está junto a main_tester.py"
        )

    logger.info("=" * 60)
    logger.info("  BUILD — compilando ros2_monitor (C++)")
    logger.info(f"  Workspace : {workspace}")
    logger.info(f"  Script    : {build_script}")
    logger.info("=" * 60)

    # ── Diagnóstico previo del entorno ───────────────────────────────────────
    _pre_checks = {
        "ROS2 Humble setup.bash": Path("/opt/ros/humble/setup.bash"),
        "workspace src/":         Path(workspace) / "src",
        "colcon en PATH":         Path(shutil.which("colcon") or ""),
    }
    for label, p in _pre_checks.items():
        estado = "OK" if p.exists() else "NO ENCONTRADO"
        level  = logger.info if p.exists() else logger.error
        level(f"  [{estado}] {label}: {p}")

    result = subprocess.run(
        ["bash", str(build_script), workspace],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,   # stderr fusionado con stdout → un solo stream
    )

    # Volcar toda la salida del script al logger, línea a línea,
    # y buscar la línea especial SETUP_BASH=...
    setup_bash_path = None
    for line in result.stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("SETUP_BASH="):
            setup_bash_path = stripped.split("=", 1)[1].strip()
        else:
            # Usar ERROR si el script ya terminó con código != 0
            log_fn = logger.error if result.returncode != 0 else logger.info
            log_fn(f"[build] {stripped}")

    if result.returncode != 0:
        raise RuntimeError(
            f"build_monitor.sh falló con código {result.returncode}."
        )

    if not setup_bash_path or not Path(setup_bash_path).exists():
        raise RuntimeError(
            f"build_monitor.sh no emitió una ruta SETUP_BASH válida "
            f"(recibido: {setup_bash_path!r})"
        )

    logger.info(f"  setup.bash → {setup_bash_path}")
    logger.info("  BUILD completado con éxito")
    logger.info("=" * 60)

    return setup_bash_path


def _patch_env_with_setup(setup_bash: str) -> dict:
    """
    Ejecuta `source <setup_bash> && env` en un subshell para obtener
    el entorno ROS2 completo y devuelve un dict con las variables.

    Se usa para que los subprocesos de ros2 run/launch/bag encuentren
    el paquete ros2_monitor recién compilado.
    """
    cmd = f"bash -c 'source {setup_bash} && env'"
    raw = subprocess.check_output(cmd, shell=True, text=True)

    env = {}
    for line in raw.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


# ──────────────────────────────────────────────
#  Directory structure check
# ──────────────────────────────────────────────
def ensure_directories(config: dict, logger: logging.Logger) -> dict:
    dirs = {}
    for name, path_str in config["directories"].items():
        p = Path(path_str).expanduser().resolve()
        p.mkdir(parents=True, exist_ok=True)
        dirs[name] = p
        logger.debug(f"Directory OK: [{name}] → {p}")

    logger.info("Directory structure verified.")
    return dirs


# ──────────────────────────────────────────────
#  Rosbag discovery
# ──────────────────────────────────────────────
def get_rosbags(test_bags_dir: Path) -> list[Path]:
    return sorted(test_bags_dir.glob("*.mcap"))


# ──────────────────────────────────────────────
#  Report writer
# ──────────────────────────────────────────────
def write_report(bag_path: Path, reports_dir: Path, failures: list, logger: logging.Logger):
    timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    first_elapsed = 0
    if failures:
        if isinstance(failures[0], dict) and "elapsed" in failures[0]:
            first_elapsed = int(failures[0]["elapsed"])

    report_name = f"report_{bag_path.stem}_{timestamp}_at_{first_elapsed}s.txt"
    report_path = reports_dir / report_name

    lines = [
        "=" * 60,
        "ROSBAG AUTOMATION TESTING — FAILURE REPORT",
        "=" * 60,
        f"Timestamp  : {datetime.now().isoformat()}",
        f"Bag file   : {bag_path.name}",
        f"Failures   : {len(failures)}",
        "-" * 60,
    ]

    for i, f in enumerate(failures, 1):
        if isinstance(f, dict):
            elapsed_str = f"{f.get('elapsed', 0.0):.1f}s"
            reason      = f.get("reason", "Unknown error")
        else:
            elapsed_str = "??.?s"
            reason      = str(f)
        lines.append(f"  [{i}] @ {elapsed_str} — {reason}")

    lines.append("=" * 60)
    report_path.write_text("\n".join(lines) + "\n")
    logger.info(f"Report written → {report_path}")
    return report_path


# ──────────────────────────────────────────────
#  Single bag simulation
# ──────────────────────────────────────────────
def run_bag(
    bag_path: Path,
    config: dict,
    dirs: dict,
    ros_env: dict,
    logger: logging.Logger,
) -> tuple[bool, list[dict]]:
    """
    Ejecuta el testeo de un rosbag.
    `ros_env` es el entorno ROS2 completo (con el workspace compilado sourced)
    que se inyecta en todos los subprocesos.
    """
    launch_cfg   = config["rosbag_launch"]
    play_cfg     = config["rosbag_play"]
    test_cfg     = config["testing"]
    checker_cfgs = config.get("checkers", [])

    launch_cmd = [
        "ros2", "launch",
        launch_cfg["package"],
        launch_cfg["launch_file"],
    ] + launch_cfg.get("extra_args", [])

    play_cmd = [
        "ros2", "bag", "play",
        str(bag_path),
    ] + play_cfg.get("extra_args", [])

    timestamp  = datetime.now().strftime("%Y%m%d_%H%M%S")
    record_dir = Path(f"/tmp/rosbag_record_{bag_path.stem}_{timestamp}")
    record_cmd = [
        "ros2", "bag", "record",
        "-o", str(record_dir),
        "--storage", "mcap",
        "-a",
    ]

    logger.info(f"  Launch cmd : {' '.join(launch_cmd)}")
    logger.info(f"  Play cmd   : {' '.join(play_cmd)}")
    logger.info(f"  Record cmd : {' '.join(record_cmd)}")

    checkers = build_checkers(checker_cfgs, logger)
    logger.info(f"  Checkers   : {[c.name for c in checkers] or 'none'}")

    proc_launch = None
    proc_play   = None
    proc_record = None

    def collect_failures() -> list[dict]:
        all_failures = []
        for checker in checkers:
            if hasattr(checker, "stop"):
                checker.stop()
            all_failures.extend(checker.failures())
        return all_failures

    def stop_process(proc, name: str):
        if proc and proc.poll() is None:
            logger.debug(f"  Terminating {name} process (PID {proc.pid})")
            proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    BAG_EXTENSIONS = {".mcap", ".db3"}

    def handle_recording():
        if not record_dir.exists():
            logger.warning("  Recording dir no encontrado — no se grabó nada.")
            return

        _recordings_dir = dirs["recordings"]
        _metadata_dir   = dirs["metadata"]
        contents        = list(record_dir.iterdir())
        logger.debug(
            f"  Record dir contents ({len(contents)} items): "
            f"{[f.name for f in contents]}"
        )

        moved = 0
        for f in contents:
            if f.suffix in BAG_EXTENSIONS:
                dest = _recordings_dir / f"{bag_path.stem}_{timestamp}{f.suffix}"
                shutil.move(str(f), str(dest))
                logger.info(f"  Recording {f.suffix} → {dest}")
                moved += 1
            elif f.name == "metadata.yaml":
                dest = _metadata_dir / f"metadata_{bag_path.stem}_{timestamp}.yaml"
                shutil.move(str(f), str(dest))
                logger.info(f"  Metadata          → {dest}")
                moved += 1
            else:
                logger.debug(f"  Ignorado: {f.name}")

        if moved == 0:
            logger.warning(
                "  Recording dir existe pero no contiene ficheros reconocidos.")

        shutil.rmtree(str(record_dir), ignore_errors=True)

    try:
        proc_launch = subprocess.Popen(
            launch_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=ros_env,        # ← entorno ROS2 con workspace sourced
        )
        logger.debug(f"  Launch PID : {proc_launch.pid}")
        time.sleep(test_cfg.get("launch_settle_seconds", 2.0))

        proc_play = subprocess.Popen(
            play_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=ros_env,
        )
        logger.debug(f"  Play PID   : {proc_play.pid}")

        proc_record = subprocess.Popen(
            record_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=ros_env,
        )
        logger.debug(f"  Record PID : {proc_record.pid}")

        for checker in checkers:
            checker.start()

        timeout       = test_cfg.get("play_timeout_seconds", None)
        poll_interval = 0.2
        elapsed       = 0.0
        failures      = []

        while True:
            if proc_play.poll() is not None:
                break
            failures = []
            for checker in checkers:
                failures.extend(checker.failures())
            if failures:
                logger.warning("  Checker failure detected — stopping simulation early.")
                break
            time.sleep(poll_interval)
            elapsed += poll_interval
            if timeout and elapsed >= timeout:
                logger.error("  Playback exceeded timeout — treating as failure.")
                break

        stop_process(proc_record, "record")
        stop_process(proc_play,   "play")
        stop_process(proc_launch, "launch")
        time.sleep(test_cfg.get("record_settle_seconds", 1.0))

        failures = collect_failures()
        handle_recording()
        return (len(failures) == 0, failures)

    except subprocess.TimeoutExpired:
        logger.error("  Playback exceeded timeout — treating as failure.")
        stop_process(proc_record, "record")
        time.sleep(test_cfg.get("record_settle_seconds", 1.0))
        failures = collect_failures()
        failures.insert(0, {"reason": "Playback exceeded configured timeout.", "elapsed": 0.0})
        handle_recording()
        return (False, failures)

    except FileNotFoundError as exc:
        logger.warning(f"  ROS2 binary not found ({exc}). Simulating dry-run.")
        time.sleep(test_cfg.get("dry_run_sleep_seconds", 1.0))
        failures = collect_failures()
        handle_recording()
        return (len(failures) == 0, failures)

    finally:
        stop_process(proc_record, "record")
        stop_process(proc_play,   "play")
        stop_process(proc_launch, "launch")
        for checker in checkers:
            if hasattr(checker, "stop"):
                checker.stop()


# ──────────────────────────────────────────────
#  Main loop
# ──────────────────────────────────────────────
def main_loop(config: dict, dirs: dict, ros_env: dict, logger: logging.Logger):
    test_bags_dir        = dirs["test_bags"]
    reports_dir          = dirs["reports"]
    cycle                = 0
    iteraciones_por_bag  = config["testing"].get("iteraciones_por_bag", 1)

    logger.info("=" * 60)
    logger.info("  ROSBAG AUTOMATION TESTING — starting infinite loop")
    logger.info("  Press Ctrl+C to stop.")
    logger.info("=" * 60)

    try:
        while True:
            cycle += 1
            bags = get_rosbags(test_bags_dir)

            if not bags:
                logger.info(
                    f"[Cycle {cycle}] No .mcap files found in {test_bags_dir}. Waiting..."
                )
                time.sleep(config["testing"].get("empty_dir_wait_seconds", 10))
                continue

            logger.info(f"[Cycle {cycle}] Found {len(bags)} bag(s) to process.")

            for bag_path in bags:
                algun_fallo    = False
                todas_los_fallos = []

                for iteracion in range(1, iteraciones_por_bag + 1):
                    logger.info(
                        f"  ▶ Processing: {bag_path.name} "
                        f"(Iteración {iteracion}/{iteraciones_por_bag})"
                    )
                    success, failures = run_bag(
                        bag_path, config, dirs, ros_env, logger
                    )
                    if success:
                        logger.info(
                            f"  ✔ PASSED — {bag_path.name} (Iteración {iteracion})"
                        )
                    else:
                        logger.warning(
                            f"  ✖ FAILED  — {bag_path.name} (Iteración {iteracion})"
                        )
                        algun_fallo = True
                        todas_los_fallos.extend(failures)

                if algun_fallo:
                    write_report(
                        bag_path=bag_path,
                        reports_dir=reports_dir,
                        failures=todas_los_fallos,
                        logger=logger,
                    )

            logger.info(f"[Cycle {cycle}] All bags processed. Restarting cycle...\n")

    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down.")


# ──────────────────────────────────────────────
#  Entry point
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Rosbag Automation Tester")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to the YAML configuration file (default: config.yaml)",
    )
    args = parser.parse_args()

    # 1. Cargar config
    try:
        cfg = load_config(args.config)
    except (FileNotFoundError, KeyError) as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    # 2. Directorios y logger
    log_dir = Path(cfg["directories"].get("logs", "logs"))
    logger  = setup_logging(log_dir)
    dirs    = ensure_directories(cfg, logger)

    # 3. Compilar el monitor C++ — ANTES de cualquier otra cosa
    try:
        setup_bash = build_cpp_monitor(cfg, logger)
    except (FileNotFoundError, RuntimeError) as e:
        logger.error(f"Error en la compilación del monitor C++: {e}")
        sys.exit(1)

    # 4. Construir el entorno ROS2 completo (workspace sourced)
    try:
        ros_env = _patch_env_with_setup(setup_bash)
    except subprocess.CalledProcessError as e:
        logger.error(f"No se pudo hacer source de {setup_bash}: {e}")
        sys.exit(1)

    # 5. Arrancar el loop principal
    main_loop(cfg, dirs, ros_env, logger)
