"""
topic_alive_checker.py

Wrapper Python sobre el nodo C++ `topic_alive_monitor`.
Mantiene la misma interfaz que BaseChecker para que el resto del sistema
(build_checkers, run_bag, etc.) no necesite ningún cambio.

Arquitectura:
  TopicAliveChecker._on_start()
      └─► subprocess: ros2 run ros2_monitor topic_alive_monitor <topic> <t>
                              │  stdout (JSON lines)
          _read_stdout() ◄───┘
              └─► _handle_event() → _record_failure() si procede

Ya no hay rclpy, nodos Python, ejecutores ni hilos de spin.
Cada checker es un proceso C++ independiente: sin GIL, sin race conditions.
"""

import json
import signal
import subprocess
import threading
import time

from .base_checker import BaseChecker


class TopicAliveChecker(BaseChecker):
    """
    Monitoriza que un topic ROS2 emita mensajes con una cadencia máxima
    de `seconds` segundos entre mensajes.

    Parámetros
    ----------
    topic   : str   Nombre completo del topic, p.ej. "/odom"
    seconds : int   Silencio máximo permitido en segundos
    logger  : opcional
    """

    # Tiempo máximo (s) que se espera al proceso C++ tras mandar SIGTERM
    _PROC_STOP_TIMEOUT = 5.0

    def __init__(self, topic: str, seconds: int, logger=None):
        super().__init__(
            name=f"TopicAliveChecker({topic})",
            logger=logger,
        )
        self.topic   = topic
        self.seconds = seconds

        self._proc         : subprocess.Popen | None = None
        self._read_thread  : threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._start_wall   : float = 0.0

    # ── Interfaz BaseChecker ──────────────────────────────────────────────────

    def _on_start(self) -> None:
        """Lanza el proceso C++ monitor y arranca el lector de stdout."""
        self._start_wall = time.monotonic()

        cmd = [
            "ros2", "run",
            "ros2_monitor", "topic_alive_monitor",
            self.topic,
            str(float(self.seconds)),
            # discovery_timeout: usar el mismo valor que timeout, con mínimo 5 s
            str(max(5.0, float(self.seconds))),
        ]

        self.logger.info(f"[{self.name}] lanzando monitor C++: {' '.join(cmd)}")

        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,          # line-buffered: cada print del C++ llega ya
        )

        self._read_thread = threading.Thread(
            target=self._read_stdout,
            daemon=True,
            name=f"mon_stdout_{self.topic}",
        )
        self._read_thread.start()

        self._stderr_thread = threading.Thread(
            target=self._drain_stderr,
            daemon=True,
            name=f"mon_stderr_{self.topic}",
        )
        self._stderr_thread.start()

    def _on_stop(self) -> None:
        """Para el proceso C++ y espera a que los hilos lectores terminen."""
        if self._proc and self._proc.poll() is None:
            self.logger.debug(f"[{self.name}] enviando SIGTERM al monitor C++")
            self._proc.send_signal(signal.SIGTERM)
            try:
                self._proc.wait(timeout=self._PROC_STOP_TIMEOUT)
            except subprocess.TimeoutExpired:
                self.logger.warning(
                    f"[{self.name}] proceso no terminó tras "
                    f"{self._PROC_STOP_TIMEOUT}s — enviando SIGKILL"
                )
                self._proc.kill()

        for thread in (self._read_thread, self._stderr_thread):
            if thread and thread.is_alive():
                thread.join(timeout=self._PROC_STOP_TIMEOUT)

    # ── Lectura de stdout ────────────────────────────────────────────────────

    def _read_stdout(self) -> None:
        """
        Lee líneas JSON del proceso C++ en un hilo dedicado.
        Termina cuando el pipe se cierra (proceso terminado).
        """
        try:
            for raw in self._proc.stdout:
                line = raw.strip()
                if not line:
                    continue

                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    # Línea de debug del C++ u otra salida no estructurada
                    self.logger.debug(
                        f"[{self.name}] stdout no-JSON: {line!r}")
                    continue

                self._handle_event(event)

                # Eventos terminales: el proceso C++ ha terminado o terminará
                # pronto, no tiene sentido seguir esperando líneas
                if event.get("event") in {"shutdown", "not_found", "error"}:
                    break

        except OSError:
            # El pipe se cerró antes de que leyéramos todo (p.ej. SIGKILL)
            pass
        except Exception as exc:
            self.logger.error(
                f"[{self.name}] error inesperado leyendo stdout: {exc}")

    def _drain_stderr(self) -> None:
        """
        Consume stderr del proceso C++ y lo vuelca al logger en DEBUG.
        Imprescindible para evitar que el buffer de stderr bloquee al proceso.
        """
        try:
            for raw in self._proc.stderr:
                line = raw.rstrip()
                if line:
                    self.logger.debug(f"[{self.name}] (C++ stderr) {line}")
        except OSError:
            pass

    # ── Manejo de eventos ────────────────────────────────────────────────────

    def _handle_event(self, event: dict) -> None:
        """Despacha cada evento JSON recibido del monitor C++."""
        evt     = event.get("event", "")
        topic   = event.get("topic", self.topic)
        elapsed = event.get("elapsed", 0.0)

        if evt == "subscribed":
            type_str = event.get("type", "tipo desconocido")
            self.logger.info(
                f"[{self.name}] suscrito a '{topic}' "
                f"[{type_str}] ({elapsed:.2f}s desde inicio)")

        elif evt == "first_message":
            self.logger.info(
                f"[{self.name}] primer mensaje en '{topic}' "
                f"({elapsed:.2f}s desde inicio)")

        elif evt == "silence":
            reason = (
                f"Silencio en '{topic}': {elapsed:.2f}s sin datos "
                f"(umbral: {self.seconds}s)"
            )
            self.logger.warning(f"[{self.name}] {reason}")
            self._record_failure(reason)

        elif evt == "not_found":
            reason = (
                f"Topic '{topic}' no encontrado tras "
                f"{elapsed:.2f}s de discovery"
            )
            self.logger.warning(f"[{self.name}] {reason}")
            self._record_failure(reason)

        elif evt == "error":
            msg    = event.get("message", "error desconocido")
            reason = f"Error en monitor de '{topic}': {msg}"
            self.logger.error(f"[{self.name}] {reason}")
            self._record_failure(reason)

        elif evt == "recovered":
            self.logger.info(
                f"[{self.name}] topic '{topic}' recuperado "
                f"({elapsed:.2f}s desde inicio)")

        elif evt == "shutdown":
            self.logger.debug(
                f"[{self.name}] monitor C++ terminó limpiamente")

        else:
            self.logger.debug(
                f"[{self.name}] evento desconocido recibido: {evt!r}")
