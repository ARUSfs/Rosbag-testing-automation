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

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    # File handler
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
    """Load and validate the YAML configuration file."""
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
#  Directory structure check
# ──────────────────────────────────────────────
def ensure_directories(config: dict, logger: logging.Logger) -> dict:
    """Ensure all required directories exist, create them if missing."""
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
    """Return a sorted list of .mcap files in the test_bags directory."""
    bags = sorted(test_bags_dir.glob("*.mcap"))
    return bags


# ──────────────────────────────────────────────
#  Report writer
# ──────────────────────────────────────────────
def write_report(bag_path: Path, reports_dir: Path, failures: list, logger: logging.Logger):
    """
    Escribe el reporte de fallos en reports/.
    Nombre: report_<bag>_<fecha>_<hora>_at_<primer_elapsed>s.txt
    failures: lista de dicts {"reason": str, "elapsed": float} o lista de strings
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Safely extract the elapsed time of the first failure
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
        # Handle both dictionary and string formats safely
        if isinstance(f, dict):
            elapsed_str = f"{f.get('elapsed', 0.0):.1f}s"
            reason = f.get("reason", "Unknown error")
        else:
            elapsed_str = "??.?s"
            reason = str(f)
            
        lines.append(f"  [{i}] @ {elapsed_str} — {reason}")
        
    lines.append("=" * 60)

    report_path.write_text("\n".join(lines) + "\n")
    logger.info(f"Report written → {report_path}")
    return report_path


# ──────────────────────────────────────────────
#  Move bag to failures
# ──────────────────────────────────────────────
def move_to_failures(bag_path: Path, failures_dir: Path, logger: logging.Logger):
    dest = failures_dir / bag_path.name
    # Avoid overwriting if a bag with the same name already failed before
    if dest.exists():
        stem = bag_path.stem
        suffix = bag_path.suffix
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = failures_dir / f"{stem}_{timestamp}{suffix}"

    shutil.move(str(bag_path), str(dest))
    logger.warning(f"Bag moved to failures → {dest}")


# ──────────────────────────────────────────────
#  Single bag simulation
# ──────────────────────────────────────────────
def run_bag(bag_path: Path, config: dict, dirs: dict, logger: logging.Logger) -> tuple[bool, list[dict]]:
    """
    Ejecuta el testeo de un rosbag:
      1. ros2 launch  — arranca la simulación
      2. ros2 bag play — reproduce el bag
      3. ros2 bag record — graba la sesión en un directorio temporal
      4. checkers — monitorizan en paralelo

    Al terminar, los ficheros del recording se distribuyen SIEMPRE:
      .mcap         → recordings/
      metadata.yaml → metadata/

    El bag original solo se mueve si hay fallos (lo gestiona main_loop):
      original .mcap → failures/

    Returns (True, []) si no hay fallos,
            (False, [lista de fallos]) si algún checker detecta algo.
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

    # Directorio temporal para la grabación
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    record_dir  = Path(f"/tmp/rosbag_record_{bag_path.stem}_{timestamp}")
    # --storage mcap fuerza formato .mcap (el defecto de ROS2 es .db3)
    record_cmd  = [
        "ros2", "bag", "record",
        "-o", str(record_dir),
        "--storage", "mcap",
        "-a",                      # graba todos los topics
    ]

    logger.info(f"  Launch cmd : {' '.join(launch_cmd)}")
    logger.info(f"  Play cmd   : {' '.join(play_cmd)}")
    logger.info(f"  Record cmd : {' '.join(record_cmd)}")

    # ── Instanciar checkers ────────────────────────────────────
    checkers = build_checkers(checker_cfgs, logger)
    logger.info(f"  Checkers   : {[c.name for c in checkers] or 'none'}")

    proc_launch = None
    proc_play   = None
    proc_record = None

    def collect_failures() -> list[dict]:
        all_failures = []
        for checker in checkers:
            # Comprobar de forma segura si el método stop existe
            if hasattr(checker, "stop"):
                checker.stop()
            all_failures.extend(checker.failures())
        return all_failures

    def stop_process(proc, name: str):
        """Detiene un proceso y su grupo sin matar al padre."""
        if proc is None:
            return

        # Comprobar si el proceso ya terminó
        if proc.poll() is not None:
            logger.debug(f"  [STOP] {name} ya había terminado.")
            return

        try:
            # Obtener el ID del grupo de procesos del hijo
            pgid = os.getpgid(proc.pid)
            
            # SEGURIDAD CRÍTICA: Nunca matar el grupo de procesos del script actual
            if pgid == os.getpgrp():
                logger.warning(f"  [STOP] ERROR DE SEGURIDAD: El grupo de {name} es igual al del padre. Abortando killpg.")
                proc.terminate() # Fallback seguro: matar solo este proceso
                return

            logger.info(f"  [STOP] Enviando SIGINT al grupo {pgid} ({name})...")
            os.killpg(pgid, signal.SIGINT)
            
            # Esperar a que los procesos cierren
            try:
                # Esperamos 5 segundos a que el grupo muera
                for _ in range(50): 
                    if proc.poll() is not None:
                        break
                    time.sleep(0.1)
            except Exception:
                pass

            # Si sigue vivo, forzamos la salida del grupo
            if proc.poll() is None:
                logger.warning(f"  [STOP] {name} persistente, forzando killpg...")
                os.killpg(pgid, signal.SIGKILL)
                
        except OSError as e:
            logger.error(f"  [STOP] Error procesando {name}: {e}")

    # Extensiones de bag que ROS2 puede generar
    BAG_EXTENSIONS = {".mcap", ".db3"}

    def handle_recording():
        """
        Tras el testeo, los ficheros del recording se distribuyen SIEMPRE:
          fichero de bag (.mcap / .db3) → recordings/
          metadata.yaml                 → metadata/

        El resultado (PASS/FAIL) no afecta aquí; solo condiciona si el bag
        original se mueve a failures/ (eso lo decide main_loop).
        """
        if not record_dir.exists():
            logger.warning("  Recording dir no encontrado — no se grabó nada.")
            return

        _recordings_dir = dirs["recordings"]
        _metadata_dir   = dirs["metadata"]

        # Log de diagnóstico: qué hay realmente en el directorio
        contents = list(record_dir.iterdir())
        logger.debug(f"  Record dir contents ({len(contents)} items): "
                     f"{[f.name for f in contents]}")

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
            logger.warning("  Recording dir existe pero no contiene ficheros de bag reconocidos.")

        shutil.rmtree(str(record_dir), ignore_errors=True)

    try:
        # ── 1. Arrancar simulación ─────────────────────────────
        proc_launch = subprocess.Popen(
            launch_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )
        logger.debug(f"  Launch PID : {proc_launch.pid}")

        time.sleep(test_cfg.get("launch_settle_seconds", 2.0))

        # ── 2. Arrancar reproducción ───────────────────────────
        proc_play = subprocess.Popen(
            play_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.debug(f"  Play PID   : {proc_play.pid}")

        # ── 3. Arrancar grabación ──────────────────────────────
        proc_record = subprocess.Popen(
            record_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        logger.debug(f"  Record PID : {proc_record.pid}")

        # ── 4. Arrancar checkers ───────────────────────────────
        for checker in checkers:
            checker.start()

        # ── 5. Monitorizar checkers y reproducción ────────────
        timeout = test_cfg.get("play_timeout_seconds", None)
        poll_interval = 0.2
        elapsed = 0.0
        failures = []
        while True:
            if proc_play.poll() is not None:
                break
            # Consultar fallos de los checkers en cada ciclo
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

        # Parar procesos si no han terminado
        stop_process(proc_record, "record")
        stop_process(proc_play,   "play")
        stop_process(proc_launch, "launch")
        time.sleep(test_cfg.get("record_settle_seconds", 1.0))

        # Recoger fallos definitivos
        failures = collect_failures()
        success  = len(failures) == 0

        # ── 8. Distribuir recording (siempre) ─────────────────
        handle_recording()
        return (success, failures)

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
        
        # NUEVO: Detener explícitamente los checkers si el usuario hace Ctrl+C
        for checker in checkers:
            if hasattr(checker, "stop"):
                checker.stop()


# ──────────────────────────────────────────────
#  Main loop
# ──────────────────────────────────────────────
def main_loop(config: dict, dirs: dict, logger: logging.Logger):

    test_bags_dir  = dirs["test_bags"]
    failures_dir   = dirs["failures"]
    reports_dir    = dirs["reports"]
    cycle          = 0

    # Permitir configurar el número de iteraciones por rosbag
    iteraciones_por_bag = config["testing"].get("iteraciones_por_bag", 1)

    logger.info("=" * 60)
    logger.info("  ROSBAG AUTOMATION TESTING — starting infinite loop")
    logger.info("  Press Ctrl+C to stop.")
    logger.info("=" * 60)

    try:
        while True:
            cycle += 1
            bags = get_rosbags(test_bags_dir)

            if not bags:
                logger.info(f"[Cycle {cycle}] No .mcap files found in {test_bags_dir}. Waiting...")
                time.sleep(config["testing"].get("empty_dir_wait_seconds", 10))
                continue

            logger.info(f"[Cycle {cycle}] Found {len(bags)} bag(s) to process.")


            for bag_path in bags:
                algun_fallo = False
                todas_las_fallos = []
                for iteracion in range(1, iteraciones_por_bag + 1):
                    logger.info(f"  ▶ Processing: {bag_path.name} (Iteración {iteracion}/{iteraciones_por_bag})")
                    success, failures = run_bag(bag_path, config, dirs, logger)
                    if success:
                        logger.info(f"  ✔ PASSED — {bag_path.name} (Iteración {iteracion})")
                    else:
                        logger.warning(f"  ✖ FAILED  — {bag_path.name} (Iteración {iteracion})")
                        algun_fallo = True
                        # Guardar los fallos de todas las iteraciones
                        todas_las_fallos.extend(failures)

                # Al terminar todas las iteraciones:
                if algun_fallo:
                    write_report(
                        bag_path=bag_path,
                        reports_dir=reports_dir,
                        failures=todas_las_fallos,
                        logger=logger,
                    )
                    move_to_failures(bag_path, failures_dir, logger)

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

    # 1. Load config
    try:
        cfg = load_config(args.config)
    except (FileNotFoundError, KeyError) as e:
        print(f"[ERROR] {e}")
        sys.exit(1)

    # 2. Ensure directories
    log_dir = Path(cfg["directories"].get("logs", "logs"))
    logger  = setup_logging(log_dir)
    dirs    = ensure_directories(cfg, logger)

    # 3. Run
    main_loop(cfg, dirs, logger)
